const { createHash, createHmac, timingSafeEqual } = require('node:crypto');

function hmac(secret, value) {
    return createHmac('sha256', secret).update(value).digest('hex');
}

function equal(left, right) {
    if (typeof left !== 'string' || typeof right !== 'string') return false;
    const a = Buffer.from(left);
    const b = Buffer.from(right);
    return a.length === b.length && timingSafeEqual(a, b);
}

function verifyRequest(app, method, url, body) {
    const parameters = Object.create(null);
    for (const [key, value] of url.searchParams) {
        const normalized = key.toLowerCase();
        if (Object.hasOwn(parameters, normalized)) return false;
        parameters[normalized] = value;
    }
    const timestamp = Number(parameters.auth_timestamp);
    if (!/^\d+$/.test(parameters.auth_timestamp || '') || !Number.isSafeInteger(timestamp)
        || Math.abs(Date.now() / 1000 - timestamp) > 600
        || parameters.auth_key !== app.key || parameters.auth_version !== '1.0') return false;
    if (body.length && !equal(parameters.body_md5, createHash('md5').update(body).digest('hex'))) return false;
    const signature = parameters.auth_signature;
    delete parameters.auth_signature;
    const canonical = Object.keys(parameters).sort().map(key => `${key}=${parameters[key]}`).join('&');
    return equal(signature, hmac(app.secret, `${method}\n${url.pathname}\n${canonical}`));
}

function verifySubscription(app, socketId, data) {
    let value = `${socketId}:${data.channel}`;
    if (data.channel.startsWith('presence-')) {
        if (typeof data.channel_data !== 'string' || Buffer.byteLength(data.channel_data) > 2048) return false;
        value += `:${data.channel_data}`;
    }
    return equal(data.auth, `${app.key}:${hmac(app.secret, value)}`);
}

module.exports = { verifyRequest, verifySubscription };
