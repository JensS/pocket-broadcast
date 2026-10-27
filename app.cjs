const path = require('node:path');
const fs = require('node:fs');
const { loadApps } = require('./src/config.cjs');
const { createService } = require('./src/server.cjs');

// Set before SQLite creates its database, WAL and shared-memory files.
process.umask(0o077);
const directory = path.resolve(process.env.REALTIME_DATA_DIR || path.join(__dirname, 'var'));
fs.mkdirSync(directory, { recursive: true, mode: 0o700 });
const apps = loadApps(process.env.REALTIME_APPS_FILE || path.join(__dirname, 'apps.json'));
const service = createService({ apps, databaseFile: path.join(directory, 'realtime.sqlite') });
service.server.listen(process.env.PORT || 3000);
process.on('SIGTERM', service.close);
process.on('SIGINT', service.close);
