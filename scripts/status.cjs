const path = require('node:path');
const { inspectStatus } = require('../src/status.cjs');

const directory = path.resolve(process.env.REALTIME_DATA_DIR || path.join(__dirname, '..', 'var'));
try {
    console.log(JSON.stringify(inspectStatus(path.join(directory, 'realtime.sqlite')), null, 2));
} catch (error) {
    console.error(`Unable to read realtime status: ${error.message}`);
    process.exitCode = 1;
}
