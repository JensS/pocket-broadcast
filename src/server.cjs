const http = require('node:http');
const { randomInt } = require('node:crypto');
const { WebSocketServer, WebSocket } = require('ws');
const { Store } = require('./store.cjs');
const { validateApps } = require('./config.cjs');
const { verifyRequest, verifySubscription } = require('./auth.cjs');

function channelValid(name) {
    return typeof name === 'string' && /^[a-zA-Z0-9_=@,.;-]{1,164}$/.test(name)
        && !name.startsWith('private-encrypted-') && !/^(private-|presence-)?cache-/.test(name);
}

function eventValid(name) {
    return typeof name === 'string' && name.length > 0 && name.length <= 200 && !/^(pusher|pusher_internal):/.test(name);
}

function createService({ apps: inputApps, databaseFile, pollMs = 250 }) {
    const apps = validateApps(inputApps);
    const store = new Store(databaseFile);
    const connections = new Map();
    const sockets = new WebSocketServer({ noServer: true, maxPayload: 16384, perMessageDeflate: false });
    let pendingRequests = 0;
    let closed = false;
    let maintained = 0;
    let lastTick = Date.now();

    function send(connection, event, data, channel, userId) {
        if (connection.socket.readyState !== WebSocket.OPEN) return;
        if (connection.socket.bufferedAmount > 1024 * 1024) {
            connection.socket.terminate();
            return;
        }
        connection.socket.send(JSON.stringify({ event, data: typeof data === 'string' ? data : JSON.stringify(data),
            ...(channel ? { channel } : {}), ...(userId !== null && userId !== undefined ? { user_id: userId } : {}) }));
    }

    function subscription(connection, data) {
        const { app, id, channels } = connection;
        const channel = data.channel;
        if (!channelValid(channel)) throw new Error('Invalid or unsupported channel');
        if (channels.has(channel)) return;
        if (channels.size >= 50) throw new Error('Subscription limit reached');
        const presence = channel.startsWith('presence-');
        if ((presence || channel.startsWith('private-')) && !verifySubscription(app, id, data)) {
            throw new Error('Subscription authorization failed');
        }
        let result = { since: store.latest() };
        let user;
        if (presence) {
            const decoded = JSON.parse(data.channel_data);
            if (!decoded || !['string', 'number'].includes(typeof decoded.user_id)
                || !String(decoded.user_id).length || String(decoded.user_id).length > 128) throw new Error('Invalid presence user');
            user = { id: String(decoded.user_id), info: decoded.user_info ?? {} };
            result = store.join(app.id, id, channel, user);
        }
        channels.set(channel, { since: result.since, user });
        send(connection, 'pusher_internal:subscription_succeeded', presence ? { presence: result.presence } : {}, channel);
    }

    function receive(connection, raw, binary) {
        try {
            const second = Math.floor(Date.now() / 1000);
            if (second !== connection.second) { connection.second = second; connection.messages = 0; }
            if (++connection.messages > 30) { connection.socket.close(1008, 'Message rate exceeded'); return; }
            if (binary) throw new Error('Text messages required');
            const message = JSON.parse(raw.toString());
            if (!message || typeof message.event !== 'string') throw new Error('Invalid event');
            const data = typeof message.data === 'string' ? JSON.parse(message.data) : message.data;
            if (message.event === 'pusher:ping') { send(connection, 'pusher:pong', {}); return; }
            if (message.event === 'pusher:pong') return;
            if (message.event === 'pusher:subscribe') { subscription(connection, data || {}); return; }
            if (message.event === 'pusher:unsubscribe') {
                if (!data || typeof data.channel !== 'string') throw new Error('Invalid channel');
                store.transaction(() => store.leave(connection.id, data.channel));
                connection.channels.delete(data.channel);
                return;
            }
            const membership = connection.channels.get(message.channel);
            if (message.event.startsWith('client-') && eventValid(message.event) && connection.app.clientEvents
                && membership && /^(private-|presence-)/.test(message.channel)) {
                const payload = JSON.stringify(data ?? null);
                if (Buffer.byteLength(payload) > 10240) throw new Error('Event is too large');
                if (++connection.clientMessages > 10) throw new Error('Client event rate exceeded');
                const status = store.publish(connection.app, [{ name: message.event, data: payload, channels: [message.channel],
                    socket_id: connection.id, user_id: membership.user?.id ?? null }]);
                if (status !== 200) throw new Error('Publishing temporarily limited');
                return;
            }
            throw new Error('Unsupported event');
        } catch (error) {
            send(connection, 'pusher:error', { code: 4009, message: error.message.startsWith('SQLITE') ? 'Service unavailable' : error.message });
        }
    }

    function json(response, status, body) {
        if (response.destroyed) return;
        response.writeHead(status, { 'Content-Type': 'application/json', 'Cache-Control': 'no-store' });
        response.end(JSON.stringify(body));
    }

    const server = http.createServer(async (request, response) => {
        if (++pendingRequests > 32) { pendingRequests--; json(response, 503, { error: 'Busy' }); request.resume(); return; }
        const timer = setTimeout(() => request.destroy(), 10000);
        try {
            const url = new URL(request.url, 'http://localhost');
            if (request.method === 'GET' && url.pathname === '/healthz') {
                store.latest();
                json(response, 200, { status: 'ok' });
                return;
            }
            const match = /^\/apps\/([a-zA-Z0-9_-]+)\/(events|batch_events)$/.exec(url.pathname);
            if (request.method !== 'POST' || !match) { json(response, 404, { error: 'Not found' }); request.resume(); return; }
            const app = apps.find(candidate => candidate.id === match[1]);
            if (!app) { json(response, 401, { error: 'Authentication failed' }); request.resume(); return; }
            const chunks = [];
            let size = 0;
            for await (const chunk of request) {
                size += chunk.length;
                if (size > 128 * 1024) { json(response, 413, { error: 'Request too large' }); request.destroy(); return; }
                chunks.push(chunk);
            }
            const body = Buffer.concat(chunks);
            if (!verifyRequest(app, request.method, url, body)) { json(response, 401, { error: 'Authentication failed' }); return; }
            let input;
            try { input = JSON.parse(body.toString()); } catch { json(response, 400, { error: 'Invalid JSON' }); return; }
            const batch = match[2] === 'batch_events' ? input?.batch : [input];
            if (!Array.isArray(batch) || batch.length < 1 || batch.length > 10) { json(response, 400, { error: 'Invalid batch' }); return; }
            const events = [];
            for (const item of batch) {
                const channels = item?.channels ?? (typeof item?.channel === 'string' ? [item.channel] : []);
                if (!item || !eventValid(item.name) || item.name.startsWith('client-') || typeof item.data !== 'string'
                    || !Array.isArray(channels) || channels.length < 1 || channels.length > 100 || !channels.every(channelValid)
                    || (item.socket_id !== undefined && !/^\d+\.\d+$/.test(item.socket_id))) {
                    json(response, 400, { error: 'Invalid event' }); return;
                }
                if (Buffer.byteLength(item.data) > 10240) { json(response, 413, { error: 'Event too large' }); return; }
                events.push({ name: item.name, data: item.data, channels: [...new Set(channels)], socket_id: item.socket_id ?? null });
            }
            const status = store.publish(app, events);
            json(response, status, status === 200 ? {} : { error: 'Publishing temporarily limited' });
        } catch {
            json(response, 503, { error: 'Service unavailable' });
        } finally {
            clearTimeout(timer);
            pendingRequests--;
        }
    });
    server.headersTimeout = 10000;
    server.requestTimeout = 10000;
    server.maxConnections = 2100;
    server.on('upgrade', (request, socket, head) => {
        let id;
        function reject(status) { socket.end(`HTTP/1.1 ${status}\r\nConnection: close\r\n\r\n`); }
        try {
            const url = new URL(request.url, 'http://localhost');
            const key = /^\/app\/([a-zA-Z0-9_-]+)$/.exec(url.pathname)?.[1];
            const app = apps.find(candidate => candidate.key === key);
            if (!app || url.searchParams.get('protocol') !== '7') { reject('400 Bad Request'); return; }
            if (!app.origins.includes(request.headers.origin)) { reject('403 Forbidden'); return; }
            if (request.headers['sec-websocket-version'] !== '13'
                || !/^[+/0-9A-Za-z]{22}==$/.test(request.headers['sec-websocket-key'] || '')) { reject('400 Bad Request'); return; }
            id = `${randomInt(1, 2 ** 48 - 1)}.${randomInt(1, 2 ** 48 - 1)}`;
            if (!store.connect(app, id)) { reject('429 Too Many Requests'); return; }
            // ws may reject an otherwise malformed upgrade after the DB reservation.
            socket.once('close', () => {
                if (!closed && !connections.has(id)) {
                    try { store.disconnect(id); } catch { fail(); }
                }
            });
            sockets.handleUpgrade(request, socket, head, websocket => {
                const connection = { id, app, socket: websocket, channels: new Map(), messages: 0, clientMessages: 0,
                    second: 0, alive: true };
                connections.set(id, connection);
                websocket.on('message', (raw, binary) => receive(connection, raw, binary));
                websocket.on('pong', () => { connection.alive = true; });
                websocket.on('error', () => {});
                websocket.on('close', () => {
                    connections.delete(id);
                    if (!closed) {
                        try { store.disconnect(id); } catch { fail(); }
                    }
                });
                send(connection, 'pusher:connection_established', { socket_id: id, activity_timeout: 30 });
            });
        } catch {
            if (id) { try { store.disconnect(id); } catch {} }
            reject('503 Service Unavailable');
        }
    });

    function tick() {
        try {
            if (Date.now() - lastTick > 45000) throw new Error('Worker stalled');
            lastTick = Date.now();
            if (Date.now() - maintained > 10000) { store.maintain(); maintained = Date.now(); }
            for (const event of store.poll()) {
                for (const connection of connections.values()) {
                    const channel = connection.channels.get(event.channel);
                    if (connection.app.id === event.app && channel && event.id > channel.since && connection.id !== event.excluded) {
                        send(connection, event.event, event.data, event.channel, event.user_id);
                    }
                }
            }
        } catch { fail(); }
    }

    function fail() {
        console.error('Realtime worker stopped: shared store unavailable or worker lease expired');
        close();
    }

    const poll = setInterval(tick, pollMs);
    const heartbeat = setInterval(() => {
        for (const connection of connections.values()) {
            if (!connection.alive) { connection.socket.terminate(); continue; }
            connection.alive = false;
            connection.socket.ping();
        }
    }, 25000);
    const clientBudget = setInterval(() => {
        for (const connection of connections.values()) connection.clientMessages = 0;
    }, 1000);

    function close() {
        if (closed) return;
        closed = true;
        clearInterval(poll); clearInterval(heartbeat); clearInterval(clientBudget);
        for (const connection of connections.values()) connection.socket.terminate();
        connections.clear();
        server.close();
        server.closeAllConnections();
        sockets.close();
        try { store.close(); } catch { /* A replacement worker will reap stale memberships. */ }
    }

    return { server, close };
}

module.exports = { createService };
