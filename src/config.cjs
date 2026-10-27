const { readFileSync } = require('node:fs');

function validateApps(apps) {
    if (!Array.isArray(apps) || apps.length === 0 || apps.length > 20) throw new Error('Configure 1–20 apps');
    const ids = new Set();
    const keys = new Set();
    return apps.map(input => {
        const app = { maxConnections: 100, publishPerSecond: 20, clientEvents: false, ...input };
        for (const name of ['id', 'key']) {
            if (typeof app[name] !== 'string' || !/^[a-zA-Z0-9_-]{1,100}$/.test(app[name])) {
                throw new Error(`Invalid app ${name}`);
            }
        }
        if (ids.has(app.id) || keys.has(app.key)) throw new Error('App ids and keys must be unique');
        ids.add(app.id);
        keys.add(app.key);
        if (typeof app.secret !== 'string' || app.secret.length < 32 || app.secret.startsWith('replace-')) {
            throw new Error('Each app needs its own random secret of at least 32 characters');
        }
        if (!Array.isArray(app.origins) || app.origins.length === 0 || !app.origins.every(origin => {
            try { return new URL(origin).origin === origin && /^https?:/.test(origin); } catch { return false; }
        })) throw new Error('Configure exact browser origins (no wildcards)');
        for (const name of ['maxConnections', 'publishPerSecond']) {
            if (!Number.isInteger(app[name]) || app[name] < 1 || app[name] > 1000) throw new Error(`Invalid ${name}`);
        }
        if (typeof app.clientEvents !== 'boolean') throw new Error('clientEvents must be boolean');
        return app;
    });
}

function loadApps(file) {
    return validateApps(JSON.parse(readFileSync(file, 'utf8')));
}

module.exports = { loadApps, validateApps };
