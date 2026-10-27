const { DatabaseSync } = require('node:sqlite');
const { statfsSync } = require('node:fs');
const database = new DatabaseSync(':memory:');
console.log(`Node ${process.version}; SQLite ${database.prepare('SELECT sqlite_version() version').get().version}; filesystem type 0x${statfsSync(process.cwd()).type.toString(16)}`);
database.close();
console.log('SQLite runtime OK');
