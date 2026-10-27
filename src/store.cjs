const { DatabaseSync } = require('node:sqlite');
const { randomUUID } = require('node:crypto');

class Store {
    constructor(file) {
        this.worker = randomUUID();
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
        `);
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

    publish(app, events) {
        return this.transaction(() => {
            // One budget shared by all Passenger workers, including optional client events.
            if (!this.rate(app.id, 'publish', app.publishPerSecond)) return 429;
            const size = this.database.prepare('SELECT COUNT(*) value FROM events').get().value;
            const added = events.reduce((sum, event) => sum + event.channels.length, 0);
            if (size + added > 5000) return 503;
            for (const event of events) {
                for (const channel of event.channels) {
                    this.enqueue(app.id, channel, event.name, event.data, event.socket_id, event.user_id);
                }
            }
            return 200;
        });
    }

    join(app, socket, channel, user) {
        return this.transaction(() => {
            const previous = this.database.prepare('SELECT COUNT(*) value FROM members WHERE app=? AND channel=? AND user_id=?')
                .get(app, channel, user.id).value;
            const count = this.database.prepare('SELECT COUNT(DISTINCT user_id) value FROM members WHERE app=? AND channel=?')
                .get(app, channel).value;
            if (!previous && count >= 100) throw new Error('Presence channel is full');
            this.database.prepare('INSERT INTO members VALUES (?,?,?,?,?)').run(socket, app, channel, user.id, JSON.stringify(user.info));
            if (!previous) this.enqueue(app, channel, 'pusher_internal:member_added',
                JSON.stringify({ user_id: user.id, user_info: user.info }), socket);
            const rows = this.database.prepare('SELECT user_id, MIN(info) info FROM members WHERE app=? AND channel=? GROUP BY user_id')
                .all(app, channel);
            return {
                since: this.latest(),
                presence: { count: rows.length, ids: rows.map(row => row.user_id),
                    hash: Object.fromEntries(rows.map(row => [row.user_id, JSON.parse(row.info)])) },
            };
        });
    }

    leave(socket, channel) {
        const rows = channel === undefined
            ? this.database.prepare('SELECT * FROM members WHERE socket=?').all(socket)
            : this.database.prepare('SELECT * FROM members WHERE socket=? AND channel=?').all(socket, channel);
        for (const row of rows) {
            this.database.prepare('DELETE FROM members WHERE socket=? AND channel=?').run(socket, row.channel);
            if (!this.database.prepare('SELECT 1 FROM members WHERE app=? AND channel=? AND user_id=? LIMIT 1')
                .get(row.app, row.channel, row.user_id)) {
                this.enqueue(row.app, row.channel, 'pusher_internal:member_removed', JSON.stringify({ user_id: row.user_id }));
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
            this.database.prepare('DELETE FROM events WHERE created<?').run(Date.now() - 60000);
        });
    }

    poll() {
        const rows = this.database.prepare('SELECT * FROM events WHERE id>? ORDER BY id LIMIT 1000').all(this.cursor);
        if (rows.length) this.cursor = rows.at(-1).id;
        return rows;
    }

    close() {
        this.transaction(() => {
            for (const { socket } of this.database.prepare('SELECT socket FROM connections WHERE worker=?').all(this.worker)) {
                this.leave(socket);
                this.database.prepare('DELETE FROM connections WHERE socket=?').run(socket);
            }
            this.database.prepare('DELETE FROM workers WHERE id=?').run(this.worker);
        });
        this.database.close();
    }
}

module.exports = { Store };
