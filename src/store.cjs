const { DatabaseSync } = require('node:sqlite');
const { createHash, randomUUID } = require('node:crypto');
const { databaseStatus } = require('./status.cjs');

const EVENT_RETENTION_MS = 60000;
const WEBHOOK_QUEUE_LIMIT = 500;

function digest(value) {
    return createHash('sha256').update(value).digest('hex');
}

class Store {
    constructor(file, apps = []) {
        this.worker = randomUUID();
        this.file = file;
        this.apps = new Map(apps.map(app => [app.id, app]));
        this.webhookDrops = 0;
        this.database = new DatabaseSync(file);
        this.database.exec(`
            PRAGMA busy_timeout=3000;
            PRAGMA journal_mode=WAL;
            PRAGMA foreign_keys=ON;
            CREATE TABLE IF NOT EXISTS workers (id TEXT PRIMARY KEY, seen INTEGER NOT NULL);
            CREATE TABLE IF NOT EXISTS connections (
                socket TEXT PRIMARY KEY, worker TEXT NOT NULL REFERENCES workers(id), app TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS connections_app ON connections(app);
            CREATE TABLE IF NOT EXISTS subscriptions (
                socket TEXT NOT NULL REFERENCES connections(socket), app TEXT NOT NULL,
                channel TEXT NOT NULL, PRIMARY KEY(socket, channel)
            );
            CREATE INDEX IF NOT EXISTS subscriptions_channel ON subscriptions(app,channel);
            CREATE TABLE IF NOT EXISTS members (
                socket TEXT NOT NULL REFERENCES connections(socket), app TEXT NOT NULL,
                channel TEXT NOT NULL, user_id TEXT NOT NULL, info TEXT NOT NULL,
                PRIMARY KEY(socket, channel)
            );
            CREATE INDEX IF NOT EXISTS members_channel ON members(app,channel,user_id);
            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT, app TEXT NOT NULL, channel TEXT NOT NULL,
                event TEXT NOT NULL, data TEXT NOT NULL, excluded TEXT, user_id TEXT, created INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS events_created ON events(created);
            CREATE TABLE IF NOT EXISTS rates (app TEXT NOT NULL, kind TEXT NOT NULL, second INTEGER NOT NULL,
                count INTEGER NOT NULL, PRIMARY KEY(app,kind));
            CREATE TABLE IF NOT EXISTS idempotency (
                app TEXT NOT NULL, key_hash TEXT NOT NULL, fingerprint TEXT NOT NULL,
                created INTEGER NOT NULL, PRIMARY KEY(app,key_hash)
            );
            CREATE INDEX IF NOT EXISTS idempotency_created ON idempotency(created);
            CREATE TABLE IF NOT EXISTS webhook_jobs (
                id TEXT PRIMARY KEY, event_id TEXT NOT NULL, app TEXT NOT NULL, endpoint TEXT NOT NULL,
                body TEXT NOT NULL, attempts INTEGER NOT NULL DEFAULT 0, available INTEGER NOT NULL,
                claimed_by TEXT, claimed_at INTEGER, last_error TEXT, status TEXT NOT NULL DEFAULT 'pending',
                created INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS webhook_pending ON webhook_jobs(status,available);
            CREATE INDEX IF NOT EXISTS webhook_created ON webhook_jobs(created);
        `);
        // Presence rows from older versions remain valid subscriptions during a rolling migration.
        this.database.exec(`INSERT OR IGNORE INTO subscriptions(socket,app,channel)
            SELECT socket,app,channel FROM members`);
        this.database.prepare('INSERT INTO workers VALUES (?,?)').run(this.worker, Date.now());
        this.cursor = this.latest();
    }

    transaction(callback) {
        this.database.exec('BEGIN IMMEDIATE');
        try {
            const result = callback();
            this.database.exec('COMMIT');
            return result;
        } catch (error) {
            this.database.exec('ROLLBACK');
            throw error;
        }
    }

    latest() {
        return this.database.prepare('SELECT COALESCE(MAX(id),0) value FROM events').get().value;
    }

    ready() {
        const worker = this.database.prepare('SELECT seen FROM workers WHERE id=?').get(this.worker);
        return Boolean(worker && worker.seen >= Date.now() - 45000);
    }

    status() {
        return { ...databaseStatus(this.database, this.file), webhookDrops: this.webhookDrops };
    }

    rate(app, kind, limit) {
        const second = Math.floor(Date.now() / 1000);
        this.database.prepare(`INSERT INTO rates VALUES (?,?,?,1) ON CONFLICT(app,kind) DO UPDATE SET
            count=CASE WHEN second=excluded.second THEN count+1 ELSE 1 END, second=excluded.second`)
            .run(app, kind, second);
        return this.database.prepare('SELECT count FROM rates WHERE app=? AND kind=?').get(app, kind).count <= limit;
    }

    connect(app, socket) {
        return this.transaction(() => {
            if (!this.rate(app.id, 'connect', 20)) return false;
            if (this.database.prepare('SELECT COUNT(*) value FROM connections WHERE app=?').get(app.id).value >= app.maxConnections
                || this.database.prepare('SELECT COUNT(*) value FROM connections').get().value >= 2000) return false;
            this.database.prepare('INSERT INTO connections VALUES (?,?,?)').run(socket, this.worker, app.id);
            return true;
        });
    }

    enqueue(app, channel, event, data, excluded = null, userId = null) {
        this.database.prepare('INSERT INTO events(app,channel,event,data,excluded,user_id,created) VALUES (?,?,?,?,?,?,?)')
            .run(app, channel, event, data, excluded, userId, Date.now());
    }

    claimMatches(claim) {
        if (!claim) return { exists: false, matches: false };
        const row = this.database.prepare('SELECT fingerprint FROM idempotency WHERE app=? AND key_hash=?')
            .get(claim.app, claim.keyHash);
        return { exists: Boolean(row), matches: row?.fingerprint === claim.fingerprint };
    }

    saveClaim(claim) {
        this.database.prepare('INSERT INTO idempotency VALUES (?,?,?,?)')
            .run(claim.app, claim.keyHash, claim.fingerprint, Date.now());
    }

    publish(app, events, requestClaim = null) {
        return this.transaction(() => {
            const normalizedRequestClaim = requestClaim ? {
                app: app.id,
                keyHash: digest(`request:${requestClaim.key}`),
                fingerprint: requestClaim.fingerprint,
            } : null;
            const requestState = this.claimMatches(normalizedRequestClaim);
            if (requestState.exists) return requestState.matches ? 200 : 409;

            const claims = events.map(event => event.idempotencyKey ? {
                app: app.id,
                keyHash: digest(`event:${event.idempotencyKey}`),
                fingerprint: event.fingerprint,
            } : null);
            const duplicates = new Set();
            const pendingClaims = new Map();
            for (let index = 0; index < events.length; index++) {
                const claim = claims[index];
                if (!claim) continue;
                if (pendingClaims.has(claim.keyHash)) {
                    if (pendingClaims.get(claim.keyHash) !== claim.fingerprint) return 409;
                    duplicates.add(index);
                    continue;
                }
                pendingClaims.set(claim.keyHash, claim.fingerprint);
                const state = this.claimMatches(claim);
                if (state.exists && !state.matches) return 409;
                if (state.matches) duplicates.add(index);
            }

            const fresh = events.filter((event, index) => !duplicates.has(index));
            if (fresh.length) {
                if (!this.rate(app.id, 'publish', app.publishPerSecond)) return 429;
                const size = this.database.prepare('SELECT COUNT(*) value FROM events').get().value;
                const added = fresh.reduce((sum, event) => sum + event.channels.length, 0);
                if (size + added > 5000) return 503;
                for (const event of fresh) {
                    for (const channel of event.channels) {
                        this.enqueue(app.id, channel, event.name, event.data, event.socket_id, event.user_id);
                    }
                }
            }
            if (normalizedRequestClaim) this.saveClaim(normalizedRequestClaim);
            for (const claim of claims) {
                if (claim && !this.claimMatches(claim).exists) this.saveClaim(claim);
            }
            return 200;
        });
    }

    enqueueWebhooks(appId, name, channel, extra = {}) {
        const app = this.apps.get(appId);
        if (!app?.webhooks?.length) return;
        const matching = app.webhooks.filter(webhook => webhook.events.includes(name));
        if (!matching.length) return;
        const eventId = randomUUID();
        const body = JSON.stringify({
            id: eventId,
            time_ms: Date.now(),
            events: [{ name, channel, ...extra }],
        });
        for (const webhook of matching) {
            const queued = this.database.prepare(`SELECT COUNT(*) count FROM webhook_jobs
                WHERE app=? AND status='pending'`).get(appId).count;
            if (queued >= WEBHOOK_QUEUE_LIMIT) {
                this.webhookDrops++;
                continue;
            }
            this.database.prepare(`INSERT INTO webhook_jobs
                (id,event_id,app,endpoint,body,attempts,available,status,created) VALUES (?,?,?,?,?,0,?,'pending',?)`)
                .run(randomUUID(), eventId, appId, webhook.url, body, Date.now(), Date.now());
        }
    }

    presence(app, channel) {
        const rows = this.database.prepare(`SELECT user_id, MIN(info) info FROM members
            WHERE app=? AND channel=? GROUP BY user_id`).all(app, channel);
        return {
            count: rows.length,
            ids: rows.map(row => row.user_id),
            hash: Object.fromEntries(rows.map(row => [row.user_id, JSON.parse(row.info)])),
        };
    }

    subscribe(appInput, socket, channel, user = null) {
        const appId = typeof appInput === 'string' ? appInput : appInput.id;
        return this.transaction(() => {
            const connection = this.database.prepare('SELECT app FROM connections WHERE socket=?').get(socket);
            if (!connection || connection.app !== appId) throw new Error('Connection is not registered');
            const occupied = this.database.prepare('SELECT 1 FROM subscriptions WHERE app=? AND channel=? LIMIT 1')
                .get(appId, channel);
            this.database.prepare('INSERT INTO subscriptions VALUES (?,?,?)').run(socket, appId, channel);
            if (!occupied) this.enqueueWebhooks(appId, 'channel_occupied', channel);
            if (user) {
                const previous = this.database.prepare(`SELECT COUNT(*) value FROM members
                    WHERE app=? AND channel=? AND user_id=?`).get(appId, channel, user.id).value;
                const count = this.database.prepare(`SELECT COUNT(DISTINCT user_id) value FROM members
                    WHERE app=? AND channel=?`).get(appId, channel).value;
                if (!previous && count >= 100) throw new Error('Presence channel is full');
                this.database.prepare('INSERT INTO members VALUES (?,?,?,?,?)')
                    .run(socket, appId, channel, user.id, JSON.stringify(user.info));
                if (!previous) {
                    this.enqueue(appId, channel, 'pusher_internal:member_added',
                        JSON.stringify({ user_id: user.id, user_info: user.info }), socket);
                    this.enqueueWebhooks(appId, 'member_added', channel, { user_id: user.id, user_info: user.info });
                }
            }
            return { since: this.latest(), presence: this.presence(appId, channel) };
        });
    }

    join(app, socket, channel, user) {
        return this.subscribe(app, socket, channel, user);
    }

    leave(socket, channel) {
        const rows = channel === undefined
            ? this.database.prepare('SELECT * FROM subscriptions WHERE socket=?').all(socket)
            : this.database.prepare('SELECT * FROM subscriptions WHERE socket=? AND channel=?').all(socket, channel);
        for (const row of rows) {
            const member = this.database.prepare('SELECT * FROM members WHERE socket=? AND channel=?')
                .get(socket, row.channel);
            if (member) {
                this.database.prepare('DELETE FROM members WHERE socket=? AND channel=?').run(socket, row.channel);
                if (!this.database.prepare('SELECT 1 FROM members WHERE app=? AND channel=? AND user_id=? LIMIT 1')
                    .get(row.app, row.channel, member.user_id)) {
                    this.enqueue(row.app, row.channel, 'pusher_internal:member_removed',
                        JSON.stringify({ user_id: member.user_id }));
                    this.enqueueWebhooks(row.app, 'member_removed', row.channel, { user_id: member.user_id });
                }
            }
            this.database.prepare('DELETE FROM subscriptions WHERE socket=? AND channel=?').run(socket, row.channel);
            if (!this.database.prepare('SELECT 1 FROM subscriptions WHERE app=? AND channel=? LIMIT 1')
                .get(row.app, row.channel)) {
                this.enqueueWebhooks(row.app, 'channel_vacated', row.channel);
            }
        }
    }

    disconnect(socket) {
        this.transaction(() => {
            this.leave(socket);
            this.database.prepare('DELETE FROM connections WHERE socket=?').run(socket);
        });
    }

    maintain() {
        this.transaction(() => {
            const stale = this.database.prepare('SELECT id FROM workers WHERE seen<?').all(Date.now() - 45000);
            for (const { id } of stale) {
                const connections = this.database.prepare('SELECT socket FROM connections WHERE worker=?').all(id);
                for (const { socket } of connections) {
                    this.leave(socket);
                    this.database.prepare('DELETE FROM connections WHERE socket=?').run(socket);
                }
                this.database.prepare('DELETE FROM workers WHERE id=?').run(id);
            }
            const changed = this.database.prepare('UPDATE workers SET seen=? WHERE id=?').run(Date.now(), this.worker);
            if (!changed.changes) throw new Error('Worker lease expired');
            this.database.prepare('DELETE FROM events WHERE created<?').run(Date.now() - EVENT_RETENTION_MS);
            this.database.prepare('DELETE FROM idempotency WHERE created<?').run(Date.now() - EVENT_RETENTION_MS);
            this.database.prepare(`DELETE FROM webhook_jobs WHERE status='dead' AND created<?`)
                .run(Date.now() - 86400000);
        });
    }

    poll() {
        const rows = this.database.prepare('SELECT * FROM events WHERE id>? ORDER BY id LIMIT 1000').all(this.cursor);
        if (rows.length) this.cursor = rows.at(-1).id;
        return rows;
    }

    channels(app, prefix = '') {
        const rows = this.database.prepare(`SELECT channel, COUNT(*) subscription_count,
            (SELECT COUNT(DISTINCT user_id) FROM members WHERE app=subscriptions.app AND channel=subscriptions.channel) user_count
            FROM subscriptions WHERE app=? AND substr(channel,1,length(?))=? GROUP BY channel ORDER BY channel`)
            .all(app, prefix, prefix);
        return Object.fromEntries(rows.map(row => [row.channel, {
            subscription_count: row.subscription_count,
            ...(row.channel.startsWith('presence-') ? { user_count: row.user_count } : {}),
        }]));
    }

    channel(app, channel) {
        const row = this.database.prepare(`SELECT COUNT(*) subscription_count,
            (SELECT COUNT(DISTINCT user_id) FROM members WHERE app=? AND channel=?) user_count
            FROM subscriptions WHERE app=? AND channel=?`).get(app, channel, app, channel);
        if (!row.subscription_count) return null;
        return {
            occupied: true,
            subscription_count: row.subscription_count,
            ...(channel.startsWith('presence-') ? { user_count: row.user_count } : {}),
        };
    }

    channelUsers(app, channel) {
        if (!channel.startsWith('presence-')) return null;
        if (!this.database.prepare('SELECT 1 FROM subscriptions WHERE app=? AND channel=? LIMIT 1').get(app, channel)) {
            return null;
        }
        return this.database.prepare(`SELECT user_id id FROM members WHERE app=? AND channel=?
            GROUP BY user_id ORDER BY user_id`).all(app, channel);
    }

    claimWebhookJobs(limit = 10) {
        return this.transaction(() => {
            const now = Date.now();
            const rows = this.database.prepare(`SELECT * FROM webhook_jobs WHERE status='pending' AND available<=?
                AND (claimed_at IS NULL OR claimed_at<?) ORDER BY available,id LIMIT ?`).all(now, now - 30000, limit);
            for (const row of rows) {
                this.database.prepare('UPDATE webhook_jobs SET claimed_by=?,claimed_at=? WHERE id=?')
                    .run(this.worker, now, row.id);
            }
            return rows;
        });
    }

    completeWebhookJob(id) {
        this.database.prepare('DELETE FROM webhook_jobs WHERE id=? AND claimed_by=?').run(id, this.worker);
    }

    failWebhookJob(id, error) {
        const job = this.database.prepare('SELECT attempts FROM webhook_jobs WHERE id=? AND claimed_by=?').get(id, this.worker);
        if (!job) return;
        const attempts = job.attempts + 1;
        const dead = attempts >= 3;
        this.database.prepare(`UPDATE webhook_jobs SET attempts=?, status=?, available=?, claimed_by=NULL,
            claimed_at=NULL, last_error=? WHERE id=? AND claimed_by=?`)
            .run(attempts, dead ? 'dead' : 'pending', Date.now() + (1000 * (2 ** job.attempts)),
                error.slice(0, 120), id, this.worker);
    }

    cleanup() {
        this.transaction(() => {
            for (const { socket } of this.database.prepare('SELECT socket FROM connections WHERE worker=?').all(this.worker)) {
                this.leave(socket);
                this.database.prepare('DELETE FROM connections WHERE socket=?').run(socket);
            }
            this.database.prepare('DELETE FROM workers WHERE id=?').run(this.worker);
        });
    }

    close() {
        this.cleanup();
        this.database.close();
    }
}

module.exports = { Store };
