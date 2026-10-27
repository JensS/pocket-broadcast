# Pocket Broadcast

A small, self-hosted WebSocket service for **Laravel Echo and the Pusher protocol**. It is designed for applications that need realtime notifications on a single Node.js server without running Redis, MySQL, or a separate message broker.

Pocket Broadcast supports multiple isolated apps, public/private/presence channels, cross-process delivery through SQLite, signed publishing, lifecycle webhooks, and the official Pusher SDKs. Laravel remains responsible for authentication, channel authorization, and authoritative application state.

> **Best-effort delivery:** Pocket Broadcast is a live notification service, not a durable event log or a complete Pusher replacement. Clients must reload authoritative state after reconnecting.

## Features

- Pusher WebSocket protocol 7 and Laravel Echo.
- Public, private, and presence channels, including multi-tab presence deduplication.
- Signed single and batch publishing through official Pusher server SDKs.
- Multiple apps with separate credentials, browser-origin allowlists, and resource limits.
- Cross-worker fanout using a shared local SQLite database.
- Optional client events and Echo whispers, disabled by default.
- Optional 60-second publish idempotency.
- Signed channel, presence-user, and operational-stat inspection endpoints.
- Optional signed lifecycle webhooks with bounded retries.
- JSON logs, health/readiness checks, a local status command, and a benchmark script.
- Graceful restarts using WebSocket close code `1012 Service Restart`.

### Intentionally unsupported

Encrypted/cache channels, HTTP/SockJS fallback transports, Pusher user authentication, event history/replay, durable realtime delivery, multi-server deployments, and Pusher SDK features outside the documented API surface. Unsupported routes return `404`.

## Requirements

- Node.js **24 or newer** with `node:sqlite` and npm.
- One server with a persistent local filesystem supporting SQLite WAL and locking.
- A process manager capable of running `app.cjs` and sending `SIGTERM` on shutdown.
- For production: HTTPS/WSS through a trusted reverse proxy that supports WebSocket upgrades.

Multiple Node processes on the **same server** can share one installation. Do not use network filesystems, object storage, ephemeral deployment directories, or multiple servers with separate SQLite files.

## Quick start

```sh
git clone https://github.com/JensS/pocket-broadcast.git
cd pocket-broadcast
npm ci
npm test
npm run create-app -- demo http://localhost:5173
npm start
```

The service listens on port `3000` by default. `create-app` writes generated credentials to the ignored `apps.json` file with mode `0600`. Add more allowed browser origins as additional arguments:

```sh
npm run create-app -- shop https://shop.example https://www.shop.example
```

Each app needs its own key and secret. Only the public key belongs in browser code. Restart all workers after changing application configuration.

## Configuration

Environment variables:

| Variable | Default | Purpose |
| --- | --- | --- |
| `PORT` | `3000` | HTTP and WebSocket listening port |
| `REALTIME_APPS_FILE` | `apps.json` beside `app.cjs` | Application configuration file |
| `REALTIME_DATA_DIR` | `var/` beside `app.cjs` | Persistent SQLite directory |

Every worker must use the **same absolute configuration and data paths**. Keep both outside the public document root and outside directories replaced during deployment.

Example application entry:

```json
{
    "id": "shop",
    "key": "public-random-key",
    "secret": "private-random-secret-at-least-32-characters",
    "origins": ["https://shop.example"],
    "maxConnections": 100,
    "publishPerSecond": 20,
    "clientEvents": false,
    "webhooks": []
}
```

See [`apps.example.json`](apps.example.json) for the complete starting shape.

## Production deployment

1. Deploy the source and install production dependencies:

   ```sh
   npm ci --omit=dev --ignore-scripts
   npm run check:runtime
   ```

2. Configure one or more apps and set `REALTIME_APPS_FILE` and `REALTIME_DATA_DIR` to persistent private paths.
3. Start `app.cjs` with Node.js 24+ under your process manager.
4. Proxy both HTTP and WebSocket traffic to the configured port. Enable TLS and preserve the WebSocket `Upgrade` and `Connection` headers.
5. Set proxy idle timeouts comfortably above the 25-second heartbeat interval.
6. Send `SIGTERM` for graceful stops and restart all workers after code or configuration changes.
7. Verify `/healthz`, `/readyz`, and the remote smoke test before sending production traffic.

Do not install dependencies during HTTP requests. Ensure the hosting account's file permissions prevent unrelated applications from reading credentials or SQLite data.

## Connect Laravel

Install the official Pusher server SDK in the Laravel application:

```sh
composer require pusher/pusher-php-server
```

Configure a Pusher broadcasting connection:

```php
'pusher' => [
    'driver' => 'pusher',
    'key' => env('PUSHER_APP_KEY'),
    'secret' => env('PUSHER_APP_SECRET'),
    'app_id' => env('PUSHER_APP_ID'),
    'options' => [
        'cluster' => 'mt1',
        'host' => env('PUSHER_HOST'),
        'port' => 443,
        'scheme' => 'https',
        'useTLS' => true,
    ],
    'client_options' => [
        'connect_timeout' => 2,
        'timeout' => 5,
    ],
],
```

Set the corresponding server environment values:

```dotenv
BROADCAST_CONNECTION=pusher
PUSHER_HOST=realtime.example
PUSHER_APP_ID=shop
PUSHER_APP_KEY=public-random-key
PUSHER_APP_SECRET=private-random-secret
```

Keep `/broadcasting/auth` in Laravel and use normal authenticated callbacks in `routes/channels.php`. Never authorize a channel solely from a submitted user ID.

Publish after the surrounding database transaction commits. For immediate notifications, use `ShouldBroadcastNow` when appropriate; ordinary queued broadcasts still depend on the Laravel queue. Handle publish failures without invalidating an already committed business operation.

## Connect Echo

Install the browser dependencies:

```sh
npm install laravel-echo pusher-js
```

Example Echo configuration:

```js
import Echo from 'laravel-echo';
import Pusher from 'pusher-js';

const echo = new Echo({
    broadcaster: 'pusher',
    Pusher,
    key: import.meta.env.VITE_PUSHER_APP_KEY,
    cluster: 'mt1',
    wsHost: import.meta.env.VITE_PUSHER_HOST,
    wsPort: 80,
    wssPort: 443,
    forceTLS: true,
    enabledTransports: ['ws', 'wss'],
    disableStats: true,
    authEndpoint: '/broadcasting/auth',
    auth: {
        headers: {
            'X-CSRF-TOKEN': document.querySelector('meta[name="csrf-token"]').content,
        },
    },
});

echo.private(`orders.${userId}`).listen('.OrderUpdated', refreshOrders);
echo.connector.pusher.connection.bind('connected', refreshOrders);
```

Every actual page origin must appear in that app's `origins`. Origins are exact matches: wildcards and missing origins are rejected. Origin checks are not authentication; private/presence authorization still supplies access control.

## HTTP API

App-specific endpoints use normal Pusher-signed HTTP authentication.

| Method | Endpoint | Purpose |
| --- | --- | --- |
| `POST` | `/apps/{id}/events` | Publish one event |
| `POST` | `/apps/{id}/batch_events` | Publish up to 10 events |
| `GET` | `/apps/{id}/channels` | List occupied channels and subscription counts |
| `GET` | `/apps/{id}/channels/{channel}` | Inspect one occupied channel |
| `GET` | `/apps/{id}/channels/{channel}/users` | List distinct users in a presence channel |
| `GET` | `/apps/{id}/stats` | Shared SQLite state and current-worker counters |

`GET /apps/{id}/channels` supports `filter_by_prefix`, for example `?filter_by_prefix=presence-`. Inspection APIs are for diagnostics and administration, not high-frequency polling.

Public operational endpoints:

| Method | Endpoint | Purpose |
| --- | --- | --- |
| `GET` | `/healthz` | Process and database liveness |
| `GET` | `/readyz` | Admission state and fresh worker lease |

## Idempotent publishing

Add an `idempotency_key` to an event or batch item:

```json
{
    "name": "OrderUpdated",
    "channel": "private-orders.42",
    "data": "{\"status\":\"paid\"}",
    "idempotency_key": "order-42-paid"
}
```

Alternatively, set `X-Idempotency-Key` to identify the complete HTTP request.

Keys must contain 1–128 characters without control characters. A retry using the same key and payload returns `200` without publishing again. Reusing a key with a different payload returns `409`. Keys are hashed before storage and expire after **60 seconds**.

Idempotency reduces duplicate notifications during retries; it does not provide durable delivery or permanent deduplication.

## Lifecycle webhooks

Webhooks are disabled by default. Configure up to five endpoints per app:

```json
{
    "url": "https://shop.example/webhooks/pocket-broadcast",
    "events": ["channel_occupied", "channel_vacated", "member_added", "member_removed"],
    "secret": "a-separate-random-webhook-secret-of-at-least-32-characters"
}
```

The optional webhook secret defaults to the app secret. Endpoints require HTTPS, except loopback HTTP for local testing. Redirects are not followed.

Each request contains a Pusher-style `time_ms` and `events` body plus a stable top-level `id`. Verify the raw-body HMAC-SHA256 hex signature in `X-Pusher-Signature`. `X-Pusher-Key` identifies the app, and `X-Pocket-Event-Id` supports receiver deduplication.

Delivery is **at-least-once**. Receivers must deduplicate, return quickly, and tolerate retries. Failed or non-2xx deliveries receive at most three attempts with short exponential backoff. Each app has a 500-job pending queue; excess lifecycle webhooks are dropped rather than blocking realtime traffic. Dead jobs are retained for 24 hours.

Webhooks can be lost after crashes, queue saturation, or exhausted retries. Never treat them as authoritative business state.

## Operations

### Status and logs

```sh
npm run status
```

The status command reports workers, connections, channels, presence, event backlog, idempotency records, webhook backlog, and database size without exposing an administrative web page.

Logs are newline-delimited JSON with timestamps, levels, and contextual fields. Secrets, signatures, and event payloads are not intentionally logged.

### Remote smoke test

Create a private test configuration containing a dedicated test app, then run from a checkout with development dependencies installed:

```sh
REALTIME_TEST_CONFIG=/private/path/test-app.json \
REALTIME_TEST_URL=https://realtime.example \
npm run smoke
```

The script reads the first app, opens a WebSocket using its first allowed origin, authorizes a private channel, publishes a signed event, and verifies delivery. It never prints the secret. HTTPS certificate validation remains enabled.

### Benchmark

```sh
npm run benchmark
BENCH_CONNECTIONS=500 BENCH_EVENTS=200 npm run benchmark
```

The benchmark uses two local workers and reports publish-to-delivery p50/p95/p99 latency. Results depend on the machine and workload and are not a production capacity guarantee.

### Shutdown behavior

A normal shutdown stops new admission, allows a one-second drain, closes sockets with `1012 Service Restart`, cleans shared worker state, and processes a bounded batch of due webhook work. Echo should reconnect, resubscribe, and refresh application state.

## Limits and delivery semantics

| Limit | Default |
| --- | --- |
| Apps per configuration | 20 |
| Connections per app | 100, configurable |
| Total connections per database | 2,000 |
| Connection attempts | 20/second/app |
| Publish requests | 20/second/app, configurable |
| Channels per publish | 100 |
| Events per batch | 10 |
| Event data | 10 KiB |
| Subscriptions per socket | 50 |
| Inbound messages per socket | 30/second |
| Client events per socket | 10/second, when enabled |
| Presence users per channel | 100 distinct users |
| Slow-consumer output buffer | 1 MiB |
| Event queue | 5,000 rows globally |
| Event and idempotency retention | 60 seconds |
| Pending webhook jobs | 500/app |

Workers poll SQLite every 250 ms, adding up to roughly that interval to delivery latency. Events are ephemeral routing records, not history. The queue returns `503` when full. Worker leases are checked every 10 seconds and stale leases expire after 45 seconds; presence cleanup after a hard crash can therefore take about 45–55 seconds.

An accepted publish does not acknowledge recipient delivery. Monitor readiness, queue depth, disk usage, publish failures, webhook backlog, and reconnect rates. Keep Laravel's database as the source of truth and never depend on realtime delivery alone for payments, orders, or other durable workflows.

## Development and verification

```sh
npm ci
npm run check:runtime
npm test
npm run benchmark
```

The integration suite uses official Pusher server/client SDKs and Laravel Echo. It covers cross-worker routing, authorization, presence, app isolation, reconnects, inspection, idempotency, webhook signatures/retries, and graceful shutdown. Tests use temporary databases and loopback listeners with synthetic credentials.

See [CONTRIBUTING.md](CONTRIBUTING.md) for contribution guidance and [SECURITY.md](SECURITY.md) for security reports.

Protocol references: [Pusher WebSocket protocol](https://pusher.com/docs/channels/library_auth_reference/pusher-websockets-protocol/), [HTTP signatures](https://pusher.com/docs/channels/library_auth_reference/rest-api/), and [channel authorization](https://pusher.com/docs/channels/library_auth_reference/auth-signatures/).

## License

MIT. Pocket Broadcast is an independent project and is not affiliated with Pusher or Laravel.
