const { readFileSync } = require('node:fs');
const { randomUUID } = require('node:crypto');
const Pusher = require('pusher');
const WebSocket = require('ws');

async function main() {
    const [app] = JSON.parse(readFileSync(process.env.REALTIME_TEST_CONFIG, 'utf8'));
    const target = new URL(process.env.REALTIME_TEST_URL);
    if (!['http:', 'https:'].includes(target.protocol) || target.pathname !== '/' || target.username || target.password) {
        throw new Error('Use an HTTP(S) origin without a path or credentials');
    }
    const publisher = new Pusher({ appId: app.id, key: app.key, secret: app.secret,
        host: target.hostname, port: Number(target.port || (target.protocol === 'https:' ? 443 : 80)),
        useTLS: target.protocol === 'https:', timeout: 5000 });
    const address = new URL(`/app/${app.key}?protocol=7&client=smoke&version=1`, target);
    address.protocol = target.protocol === 'https:' ? 'wss:' : 'ws:';
    const channel = `private-smoke-${randomUUID()}`;
    const marker = randomUUID();
    const socket = new WebSocket(address, { origin: app.origins[0], handshakeTimeout: 10000 });
    try {
        await new Promise((resolve, reject) => {
            const deadline = setTimeout(() => reject(new Error('No event received within 15 seconds')), 15000);
            function finish(error) {
                clearTimeout(deadline);
                if (error) reject(error); else resolve();
            }
            socket.on('error', () => finish(new Error('WebSocket connection failed')));
            socket.on('unexpected-response', (request, response) => {
                response.resume();
                finish(new Error(`WebSocket upgrade rejected with HTTP ${response.statusCode}`));
            });
            socket.on('close', () => finish(new Error('WebSocket closed before delivery')));
            socket.on('message', async raw => {
                try {
                    const message = JSON.parse(raw.toString());
                    const data = typeof message.data === 'string' ? JSON.parse(message.data) : message.data;
                    if (message.event === 'pusher:connection_established') {
                        const auth = publisher.authorizeChannel(data.socket_id, channel);
                        socket.send(JSON.stringify({ event: 'pusher:subscribe', data: { channel, ...auth } }));
                    } else if (message.event === 'pusher_internal:subscription_succeeded') {
                        await publisher.trigger(channel, 'smoke', { marker });
                    } else if (message.event === 'smoke' && data.marker === marker) {
                        finish();
                    } else if (message.event === 'pusher:error') {
                        finish(new Error('Service rejected the subscription'));
                    }
                } catch { finish(new Error('Authorization, publishing or event decoding failed')); }
            });
        });
        console.log('PASS: private-channel authorization, signed publishing and WebSocket delivery');
    } finally { socket.terminate(); }
}

main().catch(error => {
    console.error(error.message);
    process.exitCode = 1;
});
