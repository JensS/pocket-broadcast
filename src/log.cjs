function write(level, message, fields = {}) {
    const entry = { time: new Date().toISOString(), level, msg: message, ...fields };
    const line = JSON.stringify(entry);
    if (level === 'error' || level === 'warn') console.error(line);
    else console.log(line);
}

function logger(fields = {}) {
    return {
        debug(message, extra = {}) { write('debug', message, { ...fields, ...extra }); },
        info(message, extra = {}) { write('info', message, { ...fields, ...extra }); },
        warn(message, extra = {}) { write('warn', message, { ...fields, ...extra }); },
        error(message, extra = {}) { write('error', message, { ...fields, ...extra }); },
    };
}

module.exports = { logger };
