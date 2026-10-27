const { statSync } = require('node:fs');
const { DatabaseSync } = require('node:sqlite');

function fileSize(file) {
    try { return statSync(file).size; } catch { return 0; }
}

function databaseStatus(database, databaseFile) {
    const now = Date.now();
    const connections = database.prepare('SELECT app, COUNT(*) count FROM connections GROUP BY app ORDER BY app').all();
    const channels = database.prepare(`SELECT app, COUNT(DISTINCT channel) count, COUNT(*) subscriptions
        FROM subscriptions GROUP BY app ORDER BY app`).all();
    const queue = database.prepare('SELECT COUNT(*) count, COALESCE(MIN(created),0) oldest FROM events').get();
    const webhookQueue = database.prepare(`SELECT
        SUM(CASE WHEN status='pending' THEN 1 ELSE 0 END) pending,
        SUM(CASE WHEN status='dead' THEN 1 ELSE 0 END) dead FROM webhook_jobs`).get();
    const apps = database.prepare(`SELECT app, COUNT(DISTINCT user_id) members, COUNT(*) memberships
        FROM members GROUP BY app ORDER BY app`).all().map(row => ({
        app: row.app,
        connections: connections.find(item => item.app === row.app)?.count ?? 0,
        channels: channels.find(item => item.app === row.app)?.count ?? 0,
        subscriptions: channels.find(item => item.app === row.app)?.subscriptions ?? 0,
        presenceMembers: row.members,
        presenceMemberships: row.memberships,
    }));
    const appIds = new Set([...connections, ...channels].map(row => row.app));
    for (const app of appIds) {
        if (apps.some(item => item.app === app)) continue;
        apps.push({
            app,
            connections: connections.find(item => item.app === app)?.count ?? 0,
            channels: channels.find(item => item.app === app)?.count ?? 0,
            subscriptions: channels.find(item => item.app === app)?.subscriptions ?? 0,
            presenceMembers: 0,
            presenceMemberships: 0,
        });
    }
    apps.sort((left, right) => left.app.localeCompare(right.app));
    return {
        workers: database.prepare('SELECT COUNT(*) count FROM workers').get().count,
        staleWorkers: database.prepare('SELECT COUNT(*) count FROM workers WHERE seen<?').get(now - 45000).count,
        connections: connections.reduce((sum, row) => sum + row.count, 0),
        channels: channels.reduce((sum, row) => sum + row.count, 0),
        subscriptions: channels.reduce((sum, row) => sum + row.subscriptions, 0),
        eventQueue: {
            depth: queue.count,
            oldestAgeSeconds: queue.oldest ? Math.max(0, Math.floor((now - queue.oldest) / 1000)) : 0,
        },
        idempotencyRecords: database.prepare('SELECT COUNT(*) count FROM idempotency').get().count,
        webhooks: { pending: webhookQueue.pending ?? 0, dead: webhookQueue.dead ?? 0 },
        databaseBytes: ['', '-wal', '-shm'].reduce((sum, suffix) => sum + fileSize(`${databaseFile}${suffix}`), 0),
        apps,
    };
}

function inspectStatus(databaseFile) {
    const database = new DatabaseSync(databaseFile, { readOnly: true });
    try { return databaseStatus(database, databaseFile); } finally { database.close(); }
}

module.exports = { databaseStatus, inspectStatus };
