const { test } = require('node:test');
const assert = require('node:assert/strict');
const http = require('node:http');
const { createHash, createHmac } = require('node:crypto');
const { mkdtempSync, rmSync } = require('node:fs');
const { once } = require('node:events');
const os = require('node:os');
const path = require('node:path');
const WebSocket = require('ws');
const Pusher = require('pusher');
const { createService } = require('../src/server.cjs');
const { Store } = require('../src/store.cjs');

const pause = milliseconds => new Promise(resolve => setTimeout(resolve, milliseconds));
const quietLog = { debug() {}, info() {}, warn() {}, error() {} };

function signedUrl(worker, app, pathname, body = '', method = 'POST', headers = {}) {
    const target = new URL(`http://127.0.0.1:${worker.port}${pathname}`);
    const parameters = Object.fromEntries(target.searchParams);
    Object.assign(parameters, {
        auth_key: app.key,
        auth_timestamp: String(Math.floor(Date.now() / 1000)),
        auth_version: '1.0',
        ...(body.length ? { body_md5: createHash('md5').update(body).digest('hex') } : {}),
    });
    const canonical = Object.keys(parameters).sort().map(key => `${key}=${parameters[key]}`).join('&');
    parameters.auth_signature = createHmac('sha256', app.secret)
        .update(`${method}\n${target.pathname}\n${canonical}`).digest('hex');
    for (const key of Object.keys(target.searchParams)) target.searchParams.delete(key);
    for (const [key, value] of Object.entries(parameters)) target.searchParams.set(key, value);
    return { url: target, options: { method, body: method === 'POST' ? body : undefined, headers } };
}

async function fixture(context, appOverrides = {}) {
    const directory = mkdtempSync(path.join(os.tmpdir(), 'pocket-broadcast-advanced-'));
    const apps = [{ id: 'one', key: 'key-one', secret: `one-${'a'.repeat(40)}`,
        origins: ['https://app.test'], publishPerSecond: 100, ...appOverrides }];
    const workers = [];
    const clients = [];
    context.after(async () => {
        for (const client of clients) client.terminate();
        await Promise.all(workers.map(worker => worker.close()));
        rmSync(directory, { recursive: true, force: true });
    });
    async function worker() {
        const service = createService({ apps, databaseFile: path.join(directory, 'state.sqlite'), pollMs: 10,
            shutdownDrainMs: 0, webhookPollMs: 10, webhookTimeoutMs: 100, log: quietLog });
        workers.push(service);
        service.server.listen(0, '127.0.0.1');
        await once(service.server, 'listening');
        return { ...service, port: service.server.address().port, apps };
    }
    async function client(worker) {
        const socket = new WebSocket(`ws://127.0.0.1:${worker.port}/app/key-one?protocol=7`,
            { origin: 'https://app.test' });
        clients.push(socket);
        const messages = [];
        socket.on('message', raw => messages.push(JSON.parse(raw)));
        async function take(event) {
            const deadline = Date.now() + 3000;
            while (Date.now() < deadline) {
                const index = messages.findIndex(message => message.event === event);
                if (index >= 0) return messages.splice(index, 1)[0];
                await pause(10);
            }
            throw new Error(`Timed out waiting for ${event}`);
        }
        const hello = await take('pusher:connection_established');
        const id = JSON.parse(hello.data).socket_id;
        async function subscribe(channel, user) {
            const sdk = new Pusher({ appId: 'one', key: 'key-one', secret: apps[0].secret });
            const authorization = /^(private-|presence-)/.test(channel)
                ? sdk.authorizeChannel(id, channel, user) : {};
            socket.send(JSON.stringify({ event: 'pusher:subscribe', data: { channel, ...authorization } }));
            return take('pusher_internal:subscription_succeeded');
        }
        return { socket, messages, take, id, subscribe };
    }
    return { worker, client, apps, databaseFile: path.join(directory, 'state.sqlite') };
}

test('signed inspection and stats work across workers; restart closes with 1012', async context => {
    const f = await fixture(context);
    const first = await f.worker();
    const second = await f.worker();
    const client = await f.client(first);
    await client.subscribe('news');
    await client.subscribe('presence-room', { user_id: 'alice', user_info: { name: 'Alice' } });

    const list = signedUrl(second, f.apps[0], '/apps/one/channels?filter_by_prefix=presence-', '', 'GET');
    const listResponse = await fetch(list.url, list.options);
    assert.equal(listResponse.status, 200);
    assert.deepEqual(await listResponse.json(), {
        channels: { 'presence-room': { subscription_count: 1, user_count: 1 } },
    });
    const information = signedUrl(second, f.apps[0], '/apps/one/channels/news', '', 'GET');
    assert.deepEqual(await (await fetch(information.url, information.options)).json(),
        { occupied: true, subscription_count: 1 });
    const users = signedUrl(second, f.apps[0], '/apps/one/channels/presence-room/users', '', 'GET');
    assert.deepEqual(await (await fetch(users.url, users.options)).json(), { users: [{ id: 'alice' }] });
    const stats = signedUrl(second, f.apps[0], '/apps/one/stats', '', 'GET');
    const status = await (await fetch(stats.url, stats.options)).json();
    assert.equal(status.connections, 1);
    assert.equal(status.channels, 2);
    assert.equal(status.apps[0].presenceMembers, 1);
    assert.ok(status.worker.counters.subscriptions >= 0);
    assert.equal((await fetch(`http://127.0.0.1:${second.port}/readyz`)).status, 200);

    const closed = once(client.socket, 'close');
    await first.close();
    const [code, reason] = await closed;
    assert.equal(code, 1012);
    assert.equal(reason.toString(), 'Service restart');
});

test('event and request idempotency suppress retries and reject changed payloads', async context => {
    const f = await fixture(context);
    const worker = await f.worker();
    const client = await f.client(worker);
    await client.subscribe('news');
    const body = JSON.stringify({ name: 'Once', channel: 'news', data: '{"value":1}', idempotency_key: 'event-1' });
    for (let attempt = 0; attempt < 2; attempt++) {
        const request = signedUrl(worker, f.apps[0], '/apps/one/events', body);
        assert.equal((await fetch(request.url, request.options)).status, 200);
    }
    assert.equal(JSON.parse((await client.take('Once')).data).value, 1);
    await pause(50);
    assert.equal(client.messages.filter(message => message.event === 'Once').length, 0);

    const changed = JSON.stringify({ name: 'Once', channel: 'news', data: '{"value":2}', idempotency_key: 'event-1' });
    const conflict = signedUrl(worker, f.apps[0], '/apps/one/events', changed);
    assert.equal((await fetch(conflict.url, conflict.options)).status, 409);

    const batch = JSON.stringify({ batch: [
        { name: 'First', channel: 'news', data: '{}', idempotency_key: 'batch-1' },
        { name: 'Second', channel: 'news', data: '{}', idempotency_key: 'batch-2' },
    ] });
    for (let attempt = 0; attempt < 2; attempt++) {
        const request = signedUrl(worker, f.apps[0], '/apps/one/batch_events', batch, 'POST',
            { 'X-Idempotency-Key': 'request-1' });
        assert.equal((await fetch(request.url, request.options)).status, 200);
    }
    await client.take('First');
    await client.take('Second');
    await pause(50);
    assert.equal(client.messages.length, 0);
    const changedBatch = batch.replace('Second', 'Changed');
    const batchConflict = signedUrl(worker, f.apps[0], '/apps/one/batch_events', changedBatch, 'POST',
        { 'X-Idempotency-Key': 'request-1' });
    assert.equal((await fetch(batchConflict.url, batchConflict.options)).status, 409);

    const repeatedItem = { name: 'Repeated', channel: 'news', data: '{}', idempotency_key: 'batch-repeat' };
    const repeatedBatch = JSON.stringify({ batch: [repeatedItem, repeatedItem] });
    const repeatedRequest = signedUrl(worker, f.apps[0], '/apps/one/batch_events', repeatedBatch);
    assert.equal((await fetch(repeatedRequest.url, repeatedRequest.options)).status, 200);
    await client.take('Repeated');
    await pause(50);
    assert.equal(client.messages.filter(message => message.event === 'Repeated').length, 0);
});

test('lifecycle webhooks are signed, idempotent and retried', async context => {
    const deliveries = [];
    const receiver = http.createServer(async (request, response) => {
        const chunks = [];
        for await (const chunk of request) chunks.push(chunk);
        deliveries.push({ headers: request.headers, body: Buffer.concat(chunks).toString() });
        response.writeHead(deliveries.length === 1 ? 500 : 204);
        response.end();
    });
    receiver.listen(0, '127.0.0.1');
    await once(receiver, 'listening');
    context.after(() => receiver.close());
    const port = receiver.address().port;
    const secret = `webhook-${'b'.repeat(40)}`;
    const f = await fixture(context, { webhooks: [{ url: `http://127.0.0.1:${port}/hooks`,
        events: ['channel_occupied', 'channel_vacated', 'member_added', 'member_removed'], secret }] });
    const worker = await f.worker();
    const client = await f.client(worker);
    await client.subscribe('presence-room', { user_id: 'alice', user_info: { name: 'Alice' } });
    client.socket.close();
    const deadline = Date.now() + 5000;
    while (deliveries.length < 5 && Date.now() < deadline) await pause(20);
    assert.ok(deliveries.length >= 5, JSON.stringify(deliveries));
    const events = deliveries.map(delivery => JSON.parse(delivery.body).events[0].name);
    for (const name of ['channel_occupied', 'member_added', 'member_removed', 'channel_vacated']) {
        assert.ok(events.includes(name), `${name} missing from ${events}`);
    }
    assert.ok(deliveries.slice(1).some(delivery =>
        delivery.headers['x-pocket-event-id'] === deliveries[0].headers['x-pocket-event-id']));
    for (const delivery of deliveries) {
        const signature = createHmac('sha256', secret).update(delivery.body).digest('hex');
        assert.equal(delivery.headers['x-pusher-signature'], signature);
        assert.equal(delivery.headers['x-pusher-key'], 'key-one');
    }
});

test('webhook queue is bounded and configuration rejects remote plain HTTP', async context => {
    const directory = mkdtempSync(path.join(os.tmpdir(), 'pocket-broadcast-queue-'));
    const app = { id: 'one', key: 'key-one', secret: `one-${'a'.repeat(40)}`, origins: ['https://app.test'],
        webhooks: [{ url: 'https://hooks.example/events', events: ['channel_occupied'] }] };
    const store = new Store(path.join(directory, 'state.sqlite'), [app]);
    context.after(() => {
        if (store.database.open) store.close();
        rmSync(directory, { recursive: true, force: true });
    });
    for (let index = 0; index < 501; index++) store.enqueueWebhooks('one', 'channel_occupied', 'news');
    assert.equal(store.status().webhooks.pending, 500);
    assert.equal(store.status().webhookDrops, 1);
    assert.throws(() => createService({ apps: [{ ...app, webhooks: [{ url: 'http://hooks.example/events',
        events: ['channel_occupied'] }] }], databaseFile: path.join(directory, 'invalid.sqlite') }), /Invalid webhooks/);
});
