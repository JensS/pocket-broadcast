const fs = require('node:fs');
const path = require('node:path');
const { randomBytes } = require('node:crypto');
const { validateApps } = require('../src/config.cjs');
const [id, ...origins] = process.argv.slice(2);
if (!id || origins.length === 0) throw new Error('Usage: npm run create-app -- app-id https://app.example');
const file = process.env.REALTIME_APPS_FILE || path.join(__dirname, '..', 'apps.json');
const apps = fs.existsSync(file) ? JSON.parse(fs.readFileSync(file, 'utf8')) : [];
apps.push({ id, key: randomBytes(16).toString('hex'), secret: randomBytes(32).toString('hex'), origins,
    maxConnections: 100, publishPerSecond: 20, clientEvents: false });
validateApps(apps);
fs.writeFileSync(file, JSON.stringify(apps, null, 4) + '\n', { mode: 0o600 });
fs.chmodSync(file, 0o600);
console.log(`App saved to ${file}. Copy its id/key/secret into the Laravel server environment. Restart Node to load changes.`);
