const http = require('node:http');
const { createHash, randomInt } = require('node:crypto');
const { WebSocketServer, WebSocket } = require('ws');
const { Store } = require('./store.cjs');
const { validateApps } = require('./config.cjs');
const { verifyRequest, verifySubscription } = require('./auth.cjs');
const { createWebhookDispatcher } = require('./webhooks.cjs');
const { logger } = require('./log.cjs');

function channelValid(name) {
    return typeof name === 'string' && /^[a-zA-Z0-9_=@,.;-]{1,164}$/.test(name)
        && !name.startsWith('private-encrypted-') && !/^(private-|presence-)?cache-/.test(name);
}

function eventValid(name) {
    return typeof name === 'string' && name.length > 0 && name.length <= 200 && !/^(pusher|pusher_internal):/.test(name);
}

function idempotencyValid(value) {
    return value === undefined || (typeof value === 'string' && value.length > 0 && value.length <= 128
        && !/[\x00-\x1f\x7f]/.test(value));
}

function fingerprint(value) {
    return createHash('sha256').update(typeof value === 'string' ? value : JSON.stringify(value)).digest('hex');
}

function createService({
    apps: inputApps,
    databaseFile,
    pollMs = 250,
    shutdownDrainMs = 1000,
    webhookPollMs = 1000,
    webhookTimeoutMs = 2000,
    log = logger(),
}) {
    const apps = validateApps(inputApps);
    const store = new Store(databaseFile, apps);
    const connections = new Map();
    const subscribers = new Map();
    const sockets = new WebSocketServer({ noServer: true, maxPayload: 16384, perMessageDeflate: false });
    const webhooks = createWebhookDispatcher({ store, apps, pollMs: webhookPollMs, timeoutMs: webhookTimeoutMs, log });
    const counters = {
        connectionsAccepted: 0,
        connectionsRejected: 0,
        subscriptions: 0,
        subscriptionErrors: 0,
        publishesAccepted: 0,
        publishesRejected: 0,
        eventsSent: 0,
        clientEvents: 0,
        slowConsumers: 0,
        storeFailures: 0,
    };
    let pendingRequests = 0;
    let accepting = true;
    let closed = false;
    let closePromise = null;
    let maintained = 0;
    let lastTick = Date.now();

    function subscriptionKey(app, channel) {
        return `${app}\0${channel}`;
    }

    function indexConnection(connection, channel) {
        const key = subscriptionKey(connection.app.id, channel);
        if (!subscribers.has(key)) subscribers.set(key, new Set());
        subscribers.get(key).add(connection);
    }

    function unindexConnection(connection, channel) {
        const key = subscriptionKey(connection.app.id, channel);
        const recipients = subscribers.get(key);
        recipients?.delete(connection);
        if (recipients?.size === 0) subscribers.delete(key);
    }

    function send(connection, event, data, channel, userId) {
        if (connection.socket.readyState !== WebSocket.OPEN) return;
        if (connection.socket.bufferedAmount > 1024 * 1024) {
            counters.slowConsumers++;
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
        let user;
        if (presence) {
            const decoded = JSON.parse(data.channel_data);
            if (!decoded || !['string', 'number'].includes(typeof decoded.user_id)
                || !String(decoded.user_id).length || String(decoded.user_id).length > 128) {
                throw new Error('Invalid presence user');
            }
            user = { id: String(decoded.user_id), info: decoded.user_info ?? {} };
        }
        const result = store.subscribe(app, id, channel, user);
        channels.set(channel, { since: result.since, user });
        indexConnection(connection, channel);
        counters.subscriptions++;
        send(connection, 'pusher_internal:subscription_succeeded', presence ? { presence: result.presence } : {}, channel);
    }

    function receive(connection, raw, binary) {
        try {
            const second = Math.floor(Date.now() / 1000);
            if (second !== connection.second) {
                connection.second = second;
                connection.messages = 0;
            }
            if (++connection.messages > 30) {
                connection.socket.close(1008, 'Message rate exceeded');
                return;
            }
            if (binary) throw new Error('Text messages required');
            const message = JSON.parse(raw.toString());
            if (!message || typeof message.event !== 'string') throw new Error('Invalid event');
            const data = typeof message.data === 'string' ? JSON.parse(message.data) : message.data;
            if (message.event === 'pusher:ping') {
                send(connection, 'pusher:pong', {});
                return;
            }
            if (message.event === 'pusher:pong') return;
            if (message.event === 'pusher:subscribe') {
                subscription(connection, data || {});
                return;
            }
            if (message.event === 'pusher:unsubscribe') {
                if (!data || typeof data.channel !== 'string') throw new Error('Invalid channel');
                if (connection.channels.has(data.channel)) {
                    store.transaction(() => store.leave(connection.id, data.channel));
                    connection.channels.delete(data.channel);
                    unindexConnection(connection, data.channel);
                }
                return;
            }
            const membership = connection.channels.get(message.channel);
            if (message.event.startsWith('client-') && eventValid(message.event) && connection.app.clientEvents
                && membership && /^(private-|presence-)/.test(message.channel)) {
                const payload = JSON.stringify(data ?? null);
                if (Buffer.byteLength(payload) > 10240) throw new Error('Event is too large');
                if (++connection.clientMessages > 10) throw new Error('Client event rate exceeded');
                const status = store.publish(connection.app, [{ name: message.event, data: payload,
                    channels: [message.channel], socket_id: connection.id, user_id: membership.user?.id ?? null }]);
                if (status !== 200) throw new Error('Publishing temporarily limited');
                counters.clientEvents++;
                return;
            }
            throw new Error('Unsupported event');
        } catch (error) {
            counters.subscriptionErrors++;
            const message = /SQLITE|database|SQL/i.test(error.message) ? 'Service unavailable' : error.message;
            send(connection, 'pusher:error', { code: 4009, message });
        }
    }

    function json(response, status, body) {
        if (response.destroyed) return;
        response.writeHead(status, { 'Content-Type': 'application/json', 'Cache-Control': 'no-store' });
        response.end(JSON.stringify(body));
    }

    const server = http.createServer(async (request, response) => {
        if (++pendingRequests > 32) {
            pendingRequests--;
            json(response, 503, { error: 'Busy' });
            request.resume();
            return;
        }
        const timer = setTimeout(() => request.destroy(), 10000);
        try {
            const url = new URL(request.url, 'http://localhost');
            if (request.method === 'GET' && url.pathname === '/healthz') {
                store.latest();
                json(response, 200, { status: 'ok' });
                return;
            }
            if (request.method === 'GET' && url.pathname === '/readyz') {
                const ready = accepting && store.ready();
                json(response, ready ? 200 : 503, { status: ready ? 'ready' : 'not_ready' });
                return;
            }
            const publishMatch = /^\/apps\/([a-zA-Z0-9_-]+)\/(events|batch_events)$/.exec(url.pathname);
            const channelsMatch = /^\/apps\/([a-zA-Z0-9_-]+)\/channels$/.exec(url.pathname);
            const usersMatch = /^\/apps\/([a-zA-Z0-9_-]+)\/channels\/([^/]+)\/users$/.exec(url.pathname);
            const channelMatch = /^\/apps\/([a-zA-Z0-9_-]+)\/channels\/([^/]+)$/.exec(url.pathname);
            const statsMatch = /^\/apps\/([a-zA-Z0-9_-]+)\/stats$/.exec(url.pathname);
            const appId = publishMatch?.[1] || channelsMatch?.[1] || usersMatch?.[1] || channelMatch?.[1] || statsMatch?.[1];
            const validRoute = (request.method === 'POST' && publishMatch)
                || (request.method === 'GET' && (channelsMatch || usersMatch || channelMatch || statsMatch));
            if (!validRoute || !accepting) {
                json(response, accepting ? 404 : 503, { error: accepting ? 'Not found' : 'Service unavailable' });
                request.resume();
                return;
            }
            const app = apps.find(candidate => candidate.id === appId);
            if (!app) {
                json(response, 401, { error: 'Authentication failed' });
                request.resume();
                return;
            }
            let body = Buffer.alloc(0);
            if (request.method === 'POST') {
                const chunks = [];
                let size = 0;
                for await (const chunk of request) {
                    size += chunk.length;
                    if (size > 128 * 1024) {
                        json(response, 413, { error: 'Request too large' });
                        request.destroy();
                        return;
                    }
                    chunks.push(chunk);
                }
                body = Buffer.concat(chunks);
            }
            if (!verifyRequest(app, request.method, url, body)) {
                counters.publishesRejected++;
                log.warn('Signed HTTP request rejected', { app_id: app.id, reason: 'authentication' });
                json(response, 401, { error: 'Authentication failed' });
                return;
            }
            if (statsMatch) {
                json(response, 200, { ...store.status(), worker: { pid: process.pid, counters: { ...counters } } });
                return;
            }
            if (channelsMatch) {
                const prefix = url.searchParams.get('filter_by_prefix') || '';
                if (!/^[a-zA-Z0-9_=@,.;-]{0,164}$/.test(prefix)) {
                    json(response, 400, { error: 'Invalid channel prefix' });
                    return;
                }
                json(response, 200, { channels: store.channels(app.id, prefix) });
                return;
            }
            if (usersMatch || channelMatch) {
                let channel;
                try { channel = decodeURIComponent((usersMatch || channelMatch)[2]); } catch {
                    json(response, 400, { error: 'Invalid channel' });
                    return;
                }
                if (!channelValid(channel)) {
                    json(response, 400, { error: 'Invalid channel' });
                    return;
                }
                if (usersMatch) {
                    const users = store.channelUsers(app.id, channel);
                    if (!users) json(response, 404, { error: 'Channel not found' });
                    else json(response, 200, { users });
                    return;
                }
                const information = store.channel(app.id, channel);
                if (!information) json(response, 404, { error: 'Channel not found' });
                else json(response, 200, information);
                return;
            }

            let input;
            try { input = JSON.parse(body.toString()); } catch {
                json(response, 400, { error: 'Invalid JSON' });
                return;
            }
            const batch = publishMatch[2] === 'batch_events' ? input?.batch : [input];
            if (!Array.isArray(batch) || batch.length < 1 || batch.length > 10) {
                json(response, 400, { error: 'Invalid batch' });
                return;
            }
            const requestKey = request.headers['x-idempotency-key'];
            if (!idempotencyValid(requestKey)) {
                json(response, 400, { error: 'Invalid idempotency key' });
                return;
            }
            const events = [];
            for (const item of batch) {
                const channels = item?.channels ?? (typeof item?.channel === 'string' ? [item.channel] : []);
                if (!item || !eventValid(item.name) || item.name.startsWith('client-') || typeof item.data !== 'string'
                    || !Array.isArray(channels) || channels.length < 1 || channels.length > 100 || !channels.every(channelValid)
                    || (item.socket_id !== undefined && !/^\d+\.\d+$/.test(item.socket_id))
                    || !idempotencyValid(item.idempotency_key)) {
                    json(response, 400, { error: 'Invalid event' });
                    return;
                }
                if (Buffer.byteLength(item.data) > 10240) {
                    json(response, 413, { error: 'Event too large' });
                    return;
                }
                const event = {
                    name: item.name,
                    data: item.data,
                    channels: [...new Set(channels)].sort(),
                    socket_id: item.socket_id ?? null,
                    idempotencyKey: item.idempotency_key,
                };
                event.fingerprint = fingerprint({ ...event, idempotencyKey: undefined });
                events.push(event);
            }
            const status = store.publish(app, events, requestKey ? { key: requestKey, fingerprint: fingerprint(body) } : null);
            counters[status === 200 ? 'publishesAccepted' : 'publishesRejected']++;
            if (status !== 200) log.warn('Publish rejected', { app_id: app.id, status });
            json(response, status, status === 200 ? {} : {
                error: status === 409 ? 'Idempotency key reused with a different payload' : 'Publishing temporarily limited',
            });
        } catch (error) {
            log.error('HTTP request failed', { error: error.message.slice(0, 120) });
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
        function reject(status) {
            counters.connectionsRejected++;
            socket.end(`HTTP/1.1 ${status}\r\nConnection: close\r\n\r\n`);
        }
        try {
            if (!accepting) {
                reject('503 Service Unavailable');
                return;
            }
            const url = new URL(request.url, 'http://localhost');
            const key = /^\/app\/([a-zA-Z0-9_-]+)$/.exec(url.pathname)?.[1];
            const app = apps.find(candidate => candidate.key === key);
            if (!app || url.searchParams.get('protocol') !== '7') {
                reject('400 Bad Request');
                return;
            }
            if (!app.origins.includes(request.headers.origin)) {
                reject('403 Forbidden');
                return;
            }
            if (request.headers['sec-websocket-version'] !== '13'
                || !/^[+/0-9A-Za-z]{22}==$/.test(request.headers['sec-websocket-key'] || '')) {
                reject('400 Bad Request');
                return;
            }
            id = `${randomInt(1, 2 ** 48 - 1)}.${randomInt(1, 2 ** 48 - 1)}`;
            if (!store.connect(app, id)) {
                log.warn('Connection rejected by shared limit', { app_id: app.id });
                reject('429 Too Many Requests');
                return;
            }
            counters.connectionsAccepted++;
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
                    for (const channel of connection.channels.keys()) unindexConnection(connection, channel);
                    if (!closed) {
                        try { store.disconnect(id); } catch { fail(); }
                    }
                });
                send(connection, 'pusher:connection_established', { socket_id: id, activity_timeout: 30 });
            });
        } catch (error) {
            log.error('WebSocket upgrade failed', { error: error.message.slice(0, 120) });
            if (id) {
                try { store.disconnect(id); } catch {}
            }
            reject('503 Service Unavailable');
        }
    });

    function tick() {
        try {
            if (Date.now() - lastTick > 45000) throw new Error('Worker stalled');
            lastTick = Date.now();
            if (Date.now() - maintained > 10000) {
                store.maintain();
                maintained = Date.now();
            }
            for (const event of store.poll()) {
                const recipients = subscribers.get(subscriptionKey(event.app, event.channel));
                if (!recipients) continue;
                for (const connection of recipients) {
                    const channel = connection.channels.get(event.channel);
                    if (channel && event.id > channel.since && connection.id !== event.excluded) {
                        send(connection, event.event, event.data, event.channel, event.user_id);
                        counters.eventsSent++;
                    }
                }
            }
        } catch { fail(); }
    }

    function fail() {
        counters.storeFailures++;
        log.error('Realtime worker stopped: shared store unavailable or worker lease expired');
        close({ immediate: true }).catch(() => {});
    }

    const poll = setInterval(tick, pollMs);
    const heartbeat = setInterval(() => {
        for (const connection of connections.values()) {
            if (!connection.alive) {
                connection.socket.terminate();
                continue;
            }
            connection.alive = false;
            connection.socket.ping();
        }
    }, 25000);
    const clientBudget = setInterval(() => {
        for (const connection of connections.values()) connection.clientMessages = 0;
    }, 1000);

    function close({ immediate = false } = {}) {
        if (closePromise) return closePromise;
        accepting = false;
        server.close();
        closePromise = (async () => {
            if (!immediate && shutdownDrainMs > 0) {
                await new Promise(resolve => setTimeout(resolve, shutdownDrainMs));
            }
            closed = true;
            clearInterval(poll);
            clearInterval(heartbeat);
            clearInterval(clientBudget);
            server.closeAllConnections();
            for (const connection of connections.values()) {
                if (connection.socket.readyState === WebSocket.OPEN) connection.socket.close(1012, 'Service restart');
                const timeout = setTimeout(() => connection.socket.terminate(), 100);
                timeout.unref();
            }
            connections.clear();
            subscribers.clear();
            sockets.close();
            try { store.cleanup(); } catch (error) {
                log.error('Worker cleanup failed', { error: error.message.slice(0, 120) });
            }
            try { await webhooks.close(); } catch (error) {
                log.error('Webhook shutdown failed', { error: error.message.slice(0, 120) });
            }
            try {
                if (store.database.open) store.database.close();
            } catch {}
            log.info('Realtime worker stopped', { pid: process.pid });
        })();
        return closePromise;
    }

    return { server, close };
}

module.exports = { createService };
