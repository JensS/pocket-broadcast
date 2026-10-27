const { test } = require('node:test');
const assert = require('node:assert/strict');
const { mkdtempSync, rmSync } = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { once } = require('node:events');
const { createHmac, createHash } = require('node:crypto');
const WebSocket = require('ws');
const Pusher = require('pusher');
const PusherClient = require('pusher-js/node');
const { createService } = require('../src/server.cjs');
const { Store } = require('../src/store.cjs');
const { validateApps } = require('../src/config.cjs');
const apps = ['one', 'two'].map(id => ({ id, key: `key-${id}`, secret: `${id}-secret-${'a'.repeat(40)}`,
    origins: ['https://app.test'], clientEvents: true, publishPerSecond: 100 }));
const pause = ms => new Promise(resolve => setTimeout(resolve, ms));

async function fixture(context) {
    const directory = mkdtempSync(path.join(os.tmpdir(), 'pocket-broadcast-'));
    const databaseFile = path.join(directory, 'state.sqlite');
    const workers = [];
    const clients = [];
    context.after(async () => {
        for (const client of clients) client.terminate();
        await Promise.all(workers.map(worker => worker.close()));
        rmSync(directory, { recursive: true, force: true });
    });
    async function worker(port = 0) {
        const service = createService({ apps, databaseFile, pollMs: 10, shutdownDrainMs: 0 });
        workers.push(service);
        service.server.listen(port, '127.0.0.1');
        await once(service.server, 'listening');
        const address = service.server.address().port;
        return { ...service, port: address, sdk: new Pusher({ appId: 'one', key: apps[0].key, secret: apps[0].secret,
            host: '127.0.0.1', port: address, useTLS: false }) };
    }
    async function client(worker, app = apps[0]) {
        const socket = new WebSocket(`ws://127.0.0.1:${worker.port}/app/${app.key}?protocol=7`, { origin: 'https://app.test' });
        clients.push(socket);
        const messages = [];
        socket.on('message', raw => {
            const message = JSON.parse(raw);
            try { message.data = JSON.parse(message.data); } catch {}
            messages.push(message);
        });
        socket.on('error', () => {});
        async function take(event) {
            const end = Date.now() + 3000;
            while (Date.now() < end) {
                const index = messages.findIndex(message => message.event === event);
                if (index >= 0) return messages.splice(index, 1)[0];
                await pause(10);
            }
            throw new Error(`Timed out waiting for ${event}: ${JSON.stringify(messages)}`);
        }
        const hello = await take('pusher:connection_established');
        const id = hello.data.socket_id;
        async function subscribe(channel, user) {
            const sdk = new Pusher({ appId: app.id, key: app.key, secret: app.secret });
            const authorization = /^(private-|presence-)/.test(channel) ? sdk.authorizeChannel(id, channel, user) : {};
            socket.send(JSON.stringify({ event: 'pusher:subscribe', data: { channel, ...authorization } }));
            return take('pusher_internal:subscription_succeeded');
        }
        return { socket, messages, take, id, subscribe };
    }
    return { worker, client, databaseFile };
}

function signedUrl(worker, app, body, overrides = {}, endpoint = 'events') {
    const parameters = { auth_key: app.key, auth_timestamp: String(Math.floor(Date.now() / 1000)), auth_version: '1.0',
        body_md5: createHash('md5').update(body).digest('hex'), ...overrides };
    const pathname = `/apps/${app.id}/${endpoint}`;
    const query = Object.keys(parameters).sort().map(key => `${key}=${parameters[key]}`).join('&');
    const auth_signature = createHmac('sha256', app.secret).update(`POST\n${pathname}\n${query}`).digest('hex');
    return `http://127.0.0.1:${worker.port}${pathname}?${new URLSearchParams({ ...parameters, auth_signature })}`;
}

async function post(url, body) {
    return fetch(url, { method: 'POST', body, headers: { 'Content-Type': 'application/json' } });
}

function bound(emitter, event) {
    return new Promise((resolve, reject) => {
        const timeout = setTimeout(() => { emitter.unbind(event, handler); reject(new Error(`SDK timeout: ${event}`)); }, 6000);
        function handler(data) { clearTimeout(timeout); emitter.unbind(event, handler); resolve(data); }
        emitter.bind(event, handler);
    });
}

test('official server SDK publishes across workers; app isolation and toOthers exclusion', async context => {
    const f = await fixture(context);
    const publisher = await f.worker();
    const subscriber = await f.worker();
    const a = await f.client(subscriber);
    const b = await f.client(publisher);
    const otherApp = await f.client(subscriber, apps[1]);
    for (const client of [a, b, otherApp]) await client.subscribe('orders');
    await publisher.sdk.trigger('orders', 'OrderUpdated', { id: 7 }, { socket_id: a.id });
    assert.deepEqual((await b.take('OrderUpdated')).data, { id: 7 });
    await pause(50);
    assert.equal(a.messages.length, 0);
    assert.equal(otherApp.messages.length, 0);
    await publisher.sdk.triggerBatch([{ channel: 'orders', name: 'First', data: { value: 1 } },
        { channel: 'orders', name: 'Second', data: { value: 2 } }]);
    assert.equal((await a.take('Second')).data.value, 2);
});

test('private auth is socket-bound; ping and malformed frames do not kill the service', async context => {
    const f = await fixture(context);
    const worker = await f.worker();
    const client = await f.client(worker);
    const wrong = worker.sdk.authorizeChannel('123.456', 'private-secret');
    client.socket.send(JSON.stringify({ event: 'pusher:subscribe', data: { channel: 'private-secret', ...wrong } }));
    assert.match((await client.take('pusher:error')).data.message, /authorization/);
    await worker.sdk.trigger('private-secret', 'Hidden', { secret: 1 });
    await pause(50);
    assert.equal(client.messages.length, 0);
    await client.subscribe('private-secret');
    await worker.sdk.trigger('private-secret', 'Allowed', { ok: true });
    assert.deepEqual((await client.take('Allowed')).data, { ok: true });
    client.socket.send('{broken');
    await client.take('pusher:error');
    client.socket.send(JSON.stringify({ event: 'pusher:ping', data: {} }));
    await client.take('pusher:pong');
    assert.equal((await fetch(`http://127.0.0.1:${worker.port}/healthz`)).status, 200);
});

test('presence counts users once across workers and emits leave only after last tab', async context => {
    const f = await fixture(context);
    const first = await f.worker();
    const second = await f.worker();
    const a = await f.client(first);
    const duplicate = await f.client(second);
    const b = await f.client(second);
    await a.subscribe('presence-team', { user_id: 'alice', user_info: { name: 'Alice' } });
    const duplicateState = await duplicate.subscribe('presence-team', { user_id: 'alice', user_info: { name: 'Alice' } });
    assert.equal(duplicateState.data.presence.count, 1);
    const state = await b.subscribe('presence-team', { user_id: 'bob', user_info: { name: 'Bob' } });
    assert.equal(state.data.presence.count, 2);
    assert.equal((await a.take('pusher_internal:member_added')).data.user_id, 'bob');
    await duplicate.take('pusher_internal:member_added');
    a.socket.close();
    await once(a.socket, 'close');
    await pause(50);
    assert.equal(b.messages.length, 0);
    duplicate.socket.close();
    assert.equal((await b.take('pusher_internal:member_removed')).data.user_id, 'alice');
});

test('signed HTTP rejects tampering, expiry, duplicate auth, oversized events and partial invalid batches', async context => {
    const f = await fixture(context);
    const worker = await f.worker();
    const body = JSON.stringify({ name: 'Update', channels: ['news'], data: '{}' });
    const good = signedUrl(worker, apps[0], body);
    assert.equal((await post(good, body.replace('Update', 'Tamper'))).status, 401);
    assert.equal((await post(signedUrl(worker, apps[0], body, { auth_timestamp: '1' }), body)).status, 401);
    assert.equal((await post(`${good}&auth_key=wrong`, body)).status, 401);
    const oversized = JSON.stringify({ name: 'Update', channel: 'news', data: 'x'.repeat(10241) });
    assert.equal((await post(signedUrl(worker, apps[0], oversized), oversized)).status, 413);
    const client = await f.client(worker);
    await client.subscribe('news');
    const batch = JSON.stringify({ batch: [{ name: 'Valid', channel: 'news', data: '{}' }, { name: 'pusher:fake', channel: 'news', data: '{}' }] });
    assert.equal((await post(signedUrl(worker, apps[0], batch, {}, 'batch_events'), batch)).status, 400);
    await pause(50);
    assert.equal(client.messages.length, 0);
});

test('origin and protocol restrictions reject upgrades', async context => {
    const f = await fixture(context);
    const worker = await f.worker();
    for (const [origin, protocol, status] of [['https://evil.test', '7', 403], ['https://app.test', '6', 400]]) {
        const socket = new WebSocket(`ws://127.0.0.1:${worker.port}/app/key-one?protocol=${protocol}`, { origin });
        const response = await new Promise(resolve => {
            socket.on('unexpected-response', (request, response) => { resolve(response.statusCode); response.resume(); request.destroy(); });
            socket.on('error', () => {});
        });
        assert.equal(response, status);
    }
});

test('Echo + official pusher-js subscribes, receives private/presence events and recovers on restart', async context => {
    const { default: Echo } = await import('laravel-echo');
    const f = await fixture(context);
    let worker = await f.worker();
    // Inject a browser-equivalent Origin into the Node transport; protocol/authorization remain the official SDKs.
    const original = PusherClient.Runtime.createWebSocket;
    PusherClient.Runtime.createWebSocket = url => new WebSocket(url, { origin: 'https://app.test' });
    const client = new PusherClient('key-one', { cluster: 'mt1', wsHost: '127.0.0.1', wsPort: worker.port,
        forceTLS: false, enabledTransports: ['ws'], disableStats: true,
        channelAuthorization: { customHandler: (parameters, callback) => callback(null,
            worker.sdk.authorizeChannel(parameters.socketId, parameters.channelName,
                parameters.channelName.startsWith('presence-') ? { user_id: 'echo-user', user_info: { name: 'Echo' } } : undefined)) } });
    const echo = new Echo({ broadcaster: 'pusher', client, key: 'key-one' });
    context.after(() => { echo.disconnect(); PusherClient.Runtime.createWebSocket = original; });
    const channel = echo.private('account.1');
    await bound(channel.subscription, 'pusher:subscription_succeeded');
    const received = new Promise(resolve => channel.listen('.OrderUpdated', resolve));
    await worker.sdk.trigger('private-account.1', 'OrderUpdated', { status: 'paid' });
    assert.equal((await received).status, 'paid');
    const team = echo.join('team');
    const members = await new Promise(resolve => team.here(resolve));
    assert.deepEqual(members, [{ name: 'Echo' }]);
    const subscribedAgain = bound(channel.subscription, 'pusher:subscription_succeeded');
    const port = worker.port;
    await worker.close();
    worker = await f.worker(port);
    await subscribedAgain;
    const after = new Promise(resolve => channel.listen('.AfterRestart', resolve));
    await worker.sdk.trigger('private-account.1', 'AfterRestart', { recovered: true });
    assert.equal((await after).recovered, true);
});

test('stale worker memberships are reaped and shared connection/publish limits apply', () => {
    const directory = mkdtempSync(path.join(os.tmpdir(), 'pocket-broadcast-store-'));
    const file = path.join(directory, 'state.sqlite');
    const a = new Store(file);
    const b = new Store(file);
    try {
        const app = { ...apps[0], maxConnections: 1, publishPerSecond: 1 };
        assert.equal(a.connect(app, '1.1'), true);
        assert.equal(b.connect(app, '2.2'), false);
        a.join(app.id, '1.1', 'presence-test', { id: 'alice', info: {} });
        a.database.prepare('UPDATE workers SET seen=? WHERE id=?').run(Date.now() - 60000, a.worker);
        b.maintain();
        assert.equal(b.connect(app, '2.2'), true);
        assert.ok(b.poll().some(row => row.event === 'pusher_internal:member_removed'));
        const event = { name: 'test', data: '{}', channels: ['test'] };
        assert.equal(b.publish(app, [event]), 200);
        assert.equal(b.publish(app, [event]), 429);
    } finally { a.close(); b.close(); rmSync(directory, { recursive: true, force: true }); }
});

test('configuration rejects shared identifiers, placeholder secrets and wildcard origins', () => {
    assert.throws(() => validateApps([apps[0], apps[0]]));
    assert.throws(() => validateApps([{ ...apps[0], origins: ['*'] }]));
    assert.throws(() => validateApps([{ ...apps[0], secret: 'replace-with-at-least-32-random-bytes' }]));
});

test('client whispers require an authorized subscription and never echo to the sender', async context => {
    const f = await fixture(context);
    const worker = await f.worker();
    const sender = await f.client(worker);
    const recipient = await f.client(worker);
    await sender.subscribe('private-chat');
    await recipient.subscribe('private-chat');
    sender.socket.send(JSON.stringify({ event: 'client-typing', channel: 'private-chat', data: { typing: true } }));
    assert.deepEqual((await recipient.take('client-typing')).data, { typing: true });
    await pause(30);
    assert.equal(sender.messages.length, 0);
    sender.socket.send(JSON.stringify({ event: 'client-typing', channel: 'private-unjoined', data: {} }));
    assert.match((await sender.take('pusher:error')).data.message, /Unsupported/);
});

test('a paused worker fails closed instead of publishing stale presence or missing history silently', async context => {
    const f = await fixture(context);
    const worker = await f.worker();
    const client = await f.client(worker);
    await client.subscribe('presence-workers', { user_id: 'user', user_info: {} });
    const control = new Store(f.databaseFile);
    control.database.prepare('UPDATE workers SET seen=? WHERE id!=?').run(Date.now() - 60000, control.worker);
    control.maintain();
    // The retired worker is detected on its next lease maintenance, not merely by polling events.
    const closed = once(client.socket, 'close');
    let timeout;
    try {
        await Promise.race([closed, new Promise((resolve, reject) => {
            timeout = setTimeout(() => reject(new Error('Retired worker remained connected')), 12000);
        })]);
        assert.equal(control.database.prepare('SELECT COUNT(*) value FROM members').get().value, 0);
    } finally { clearTimeout(timeout); control.close(); }
});
