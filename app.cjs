const path = require('node:path');
const fs = require('node:fs');
const { loadApps } = require('./src/config.cjs');
const { createService } = require('./src/server.cjs');
const { logger } = require('./src/log.cjs');

const log = logger({ pid: process.pid });

// Set before SQLite creates its database, WAL and shared-memory files.
process.umask(0o077);
const directory = path.resolve(process.env.REALTIME_DATA_DIR || path.join(__dirname, 'var'));
fs.mkdirSync(directory, { recursive: true, mode: 0o700 });
const apps = loadApps(process.env.REALTIME_APPS_FILE || path.join(__dirname, 'apps.json'));
const service = createService({ apps, databaseFile: path.join(directory, 'realtime.sqlite'), log });
service.server.listen(process.env.PORT || 3000, () => {
    const address = service.server.address();
    log.info('Realtime worker listening', { address: typeof address === 'string' ? address : `${address.address}:${address.port}` });
});
for (const signal of ['SIGTERM', 'SIGINT']) {
    process.on(signal, () => {
        service.close().catch(error => {
            log.error('Realtime shutdown failed', { error: error.message });
            process.exitCode = 1;
        });
    });
}
