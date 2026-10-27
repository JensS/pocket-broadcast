const { mkdtempSync, rmSync } = require('node:fs');
const { once } = require('node:events');
const os = require('node:os');
const path = require('node:path');
const WebSocket = require('ws');
const Pusher = require('pusher');
const { createService } = require('../src/server.cjs');

const connectionTarget = Number(process.env.BENCH_CONNECTIONS || 100);
const eventTarget = Number(process.env.BENCH_EVENTS || 100);
const pause = milliseconds => new Promise(resolve => setTimeout(resolve, milliseconds));

function percentile(values, fraction) {
    const sorted = [...values].sort((left, right) => left - right);
    return sorted[Math.min(sorted.length - 1, Math.floor(sorted.length * fraction))];
}

async function main() {
    if (!Number.isInteger(connectionTarget) || connectionTarget < 1 || connectionTarget > 1000
        || !Number.isInteger(eventTarget) || eventTarget < 1 || eventTarget > 1000) {
        throw new Error('Use 1–1000 connections and events');
    }
    const directory = mkdtempSync(path.join(os.tmpdir(), 'pocket-broadcast-bench-'));
    const apps = [{ id: 'bench', key: 'bench-key', secret: `bench-${'a'.repeat(40)}`,
        origins: ['https://bench.test'], maxConnections: 1000, publishPerSecond: 1000 }];
    const quietLog = { debug() {}, info() {}, warn() {}, error: console.error };
    const options = { apps, databaseFile: path.join(directory, 'state.sqlite'), pollMs: 5,
        shutdownDrainMs: 0, log: quietLog };
    const publisher = createService(options);
    const subscriber = createService(options);
    const clients = [];
    try {
        for (const service of [publisher, subscriber]) {
            service.server.listen(0, '127.0.0.1');
            await once(service.server, 'listening');
        }
        const port = subscriber.server.address().port;
        const messages = [];
        for (let index = 0; index < connectionTarget; index++) {
            const socket = new WebSocket(`ws://127.0.0.1:${port}/app/bench-key?protocol=7`,
                { origin: 'https://bench.test' });
            clients.push(socket);
            const hello = new Promise((resolve, reject) => {
                socket.on('error', reject);
                socket.on('message', function listen(raw) {
                    const message = JSON.parse(raw);
                    if (message.event !== 'pusher:connection_established') return;
                    socket.off('message', listen);
                    resolve(message);
                });
            });
            await hello;
            await pause(55); // Stay below the shared 20 connections/second admission limit.
            if (index === 0) socket.on('message', raw => messages.push(JSON.parse(raw)));
        }
        const subscribed = new Promise(resolve => {
            const check = () => {
                if (messages.some(message => message.event === 'pusher_internal:subscription_succeeded')) resolve();
            };
            clients[0].on('message', check);
        });
        clients[0].send(JSON.stringify({ event: 'pusher:subscribe', data: { channel: 'benchmark' } }));
        await subscribed;
        const sdk = new Pusher({ appId: 'bench', key: 'bench-key', secret: apps[0].secret,
            host: '127.0.0.1', port: publisher.server.address().port, useTLS: false });
        const latencies = [];
        for (let index = 0; index < eventTarget; index++) {
            const started = performance.now();
            await sdk.trigger('benchmark', 'Benchmark', { index });
            while (!messages.some(message => message.event === 'Benchmark'
                && JSON.parse(message.data).index === index)) await pause(1);
            latencies.push(performance.now() - started);
        }
        console.log(JSON.stringify({
            workers: 2,
            connections: connectionTarget,
            events: eventTarget,
            latencyMs: {
                p50: Number(percentile(latencies, 0.5).toFixed(2)),
                p95: Number(percentile(latencies, 0.95).toFixed(2)),
                p99: Number(percentile(latencies, 0.99).toFixed(2)),
            },
            note: 'Local synthetic cross-worker publish-to-delivery; not a hosting capacity guarantee',
        }, null, 2));
    } finally {
        for (const client of clients) client.terminate();
        await Promise.all([publisher.close(), subscriber.close()]);
        rmSync(directory, { recursive: true, force: true });
    }
}

main().catch(error => {
    console.error(error);
    process.exitCode = 1;
});
