# Pocket Broadcast

A small, independently deployed Pusher-protocol service for Laravel Echo on shared Node hosting, including netcup/Plesk. Laravel, its database and channel authorization stay on the PHP webspace. It runs as a standalone application alongside your Laravel projects.

**Status:** initial implementation, not a full Pusher replacement. Run the tests and the hosting smoke check before using it. Production needs a dedicated hostname with a trusted certificate. The earlier preflight proved plain WebSockets and restart recovery, not production TLS or capacity.

Verified on 2026-10-27: ten local integration tests pass using the official Pusher server/client SDKs and Laravel Echo, including routing between workers, signed requests, private/presence channels, client whispers, app isolation and reconnect/resubscribe after restart. The standalone smoke script passes locally. The production dependency audit reports no known vulnerabilities. Netcup's live Passenger application returns a healthy response using Node 26.12.1 and SQLite 3.54.0; private config/database URLs return 404. The hosted private-channel smoke test passes with nginx Proxy mode disabled. The official client also reconnects, reauthorizes, resubscribes and receives an event after a real panel restart. Hosting tests used synthetic data over plain WebSockets; production WSS is not yet verified. These checks do not establish production capacity or long-term availability.

## Supported scope

- Pusher WebSocket protocol 7; public, private and presence channels.
- Laravel Echo `channel`, `private`, `join`, notifications and `toOthers` socket exclusion.
- Signed `POST /apps/{id}/events` and `/batch_events` using official Pusher server SDKs.
- Optional `client-*` events / Echo whispers on authorized private or presence channels (`clientEvents: true`). Off by default.
- Multiple isolated apps, exact browser-origin allowlists, heartbeat, message-size, connection and rate limits.
- Presence deduplicates multiple tabs for the same user. Membership is removed on disconnect or after a dead worker's lease expires.

Not implemented: encrypted/cache channels, HTTP/SockJS fallbacks, user sign-in, webhooks, channel-inspection REST endpoints, event replay, durable delivery or Pusher's extended `info` response. REST publishing uses JSON bodies, as Laravel's SDK does. Unsupported endpoints return 404. Do not enable SDK features outside this scope.

## Why SQLite

Passenger may route a publish request and its subscribers to different processes. Each process uses the **same absolute SQLite file on local disk**, with WAL locking, for a short event queue, connection limits and presence membership. No Redis, MySQL account or separate background daemon is needed. The browser uses WebSockets; workers check this queue every 250 ms, adding up to roughly that interval to delivery latency.

Events expire after 60 seconds; the publishing queue is capped at 5,000 rows (publish returns 503 when full). Connection/presence rows and lifecycle notifications also use this database. Disk usage depends on the high-water mark, WAL checkpointing and churn. It is for modest traffic, not a substitute for a dedicated broker. SQLite must be on a filesystem supporting SQLite locking/WAL; **do not put it on S3 or a network filesystem**. Benchmark your intended traffic. Node's built-in SQLite API may emit an experimental warning.

A worker heartbeats its lease every 10 seconds. Other workers reap leases older than 45 seconds. A stalled/retired worker closes its connections rather than silently continuing with stale membership. Presence may therefore lag a hard crash by about 45–55 seconds. If all workers stop, cleanup occurs when a new worker starts.

## Local setup

Requires Node **24+** with `node:sqlite` and npm.

```sh
git clone https://github.com/JensS/pocket-broadcast.git
cd pocket-broadcast
npm ci
npm test
npm run create-app -- demo http://localhost:5173
npm start
```

App credentials are saved in ignored `apps.json` with mode 0600. Each app gets a separate key and secret. Add multiple origins as additional arguments. Restart all workers after editing app configuration. Keep only the public key in browser code; the secret belongs in Laravel's server environment and this service's private config.

Optional environment variables:

| Variable | Default |
|---|---|
| `PORT` | `3000` locally; Passenger manages the actual listening socket |
| `REALTIME_APPS_FILE` | `apps.json` beside `app.cjs` |
| `REALTIME_DATA_DIR` | `var/` beside `app.cjs` |

Every worker **must use the same config and data directory**. Keep them outside the public document root and deployment cleanup paths. Do not run two differently configured services against the same data directory. File permissions protect them from other hosting users, not from other applications using the same hosting account.

## netcup / Plesk deployment

### Automatic deployments

The GitHub workflow tests pushes to `main` on Node 24 and 26. After both pass,
it publishes the source plus locked production dependencies to the generated
`plesk-deploy` branch. Failed tests do not update that branch. Do not edit it by
hand; `DEPLOYED_REVISION` records the source commit.

In Plesk's Git extension, connect this repository over HTTPS, select
`plesk-deploy`, and choose automatic deployment to the application root. Add its
generated webhook URL as a GitHub **push** webhook with SSL verification enabled.
Use an additional deployment action to create `tmp/restart.txt` in the application
root so Passenger reloads it. Keep `.plesk.startup.cjs`, `apps.json` and `var/`
untracked; the published branch contains none of these runtime files. No npm
command or GitHub SSH key is needed on the hosting server.

This is file deployment, not a zero-downtime release switch. Existing WebSockets
may reconnect during restart. Roll back by reverting the source commit on `main`;
the revert goes through the same tests and deployment. An HTTP health check should
be part of the host's deployment actions.

### Initial setup

Use a **separate subdomain and app directory**; do not switch the Laravel domain away from Apache.

1. Create the subdomain and set its document root to the service's empty `public/` directory. Keep `apps.json`, source and SQLite files outside that root. Install and select a valid certificate for this hostname.
2. Upload this directory, excluding `node_modules`, `test`, local `var`, and sample credentials. Transfer the generated `apps.json` privately. Keep a backup of it.
3. Find **Node.js** through Plesk's global search (the domain dashboard may hide its link). Select Node 24+; set the application root to this directory and the startup file to **app.cjs**. Run `npm ci --omit=dev --ignore-scripts` through the package command runner, then `npm run check:runtime`.
4. Under **Apache & nginx settings**, disable **Proxy mode** for this subdomain. Enable Node. If Plesk's automatic configuration is used, inspect its generated startup wrapper and make sure it loads `app.cjs`. Remove/rename only the default placeholder `public/index.html`, if one exists.
5. Set `REALTIME_DATA_DIR` to an absolute private directory outside any release checkout. Do the same for `REALTIME_APPS_FILE` if deploying by replacing directories. The Node process and SSH can see different absolute paths; use the paths visible to the panel runtime.
6. Verify `https://realtime.example/healthz` and run the remote smoke test below. Test with at least two app credentials and origins before expanding use.

No PM2, cron, Supervisor or `node` in the SSH PATH is required. The panel also omits `node` from npm scripts’ PATH; `check:runtime` uses npm’s `npm_node_execpath` to handle this (POSIX shells). The panel runs npm and Passenger starts the application on demand. Never download/install dependencies during an HTTP request. Restart through **App neu starten** after code or config updates. Connections drop and Echo reconnects/resubscribes automatically.

### Remote smoke test

From this directory locally (with dev dependencies installed), provide a private config containing a dedicated test app. The script reads its first entry and never prints the secret:

```sh
REALTIME_TEST_CONFIG=/private/path/test-app.json \
REALTIME_TEST_URL=https://realtime.example \
npm run smoke
```

It opens a WebSocket using that app's first allowed origin, subscribes to a private channel using the official server SDK's authorization, publishes a signed event and checks delivery. HTTPS validation stays enabled. The script also supports `http://` for synthetic preflight testing only; production browsers on HTTPS require WSS.

## Laravel connection

Install `pusher/pusher-php-server` in each Laravel app if it is not already present. Configure a `pusher` broadcasting connection using that app's credentials:

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
    'client_options' => ['connect_timeout' => 2, 'timeout' => 5],
],
```

Set `BROADCAST_CONNECTION=pusher`, `PUSHER_HOST=realtime.example`, and the three matching credentials in the server environment. Keep `/broadcasting/auth` on Laravel and use normal authenticated channel callbacks in `routes/channels.php`; never authorize channels based solely on a submitted user ID.

For immediate lightweight notifications, implement **`ShouldBroadcastNow`**. Ordinary `ShouldBroadcast` events remain subject to the app's queue, including the every-minute netcup cron delay. Publish only after the associated database transaction commits (for example, dispatch from `DB::afterCommit`). A failed synchronous publish must not invalidate an already completed payment/order operation: handle/report the failure appropriately and let clients refresh authoritative state.

## Echo client

Install `laravel-echo` and `pusher-js` in the Laravel frontend build. Build assets locally, as with the other netcup projects.

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

Configure each actual page origin (`https://app.example`, and `https://www.app.example` if used) in that app's `origins`. They are exact matches; no wildcard and no missing Origin are accepted. Origin filtering is not authentication: public channels are public. Private/presence channel authorization supplies access control.

## Operational limits

Defaults per app: 100 concurrent sockets, 20 publish requests/second. Limits are shared between workers. Each publish can target at most 100 channels; a batch can contain at most 10 events. Event data is limited to 10 KiB. Each socket has at most 50 subscriptions, 30 inbound messages/second and optionally 10 client events/second. Presence channels support 100 distinct users. Slow sockets with over 1 MiB queued output are disconnected. There is also a global cap of 2,000 sockets per database.

The service is **best effort live notification delivery**. An accepted HTTP publish is not an acknowledgement from every recipient. Restarted/disconnected clients must fetch current state from Laravel. Use your application database as the source of truth; do not depend on WebSocket events for payment processing or other durable work. Monitor `/healthz`, disk usage, publish failures and client reconnect rates. The host's connection/CPU limits still apply.

Protocol references: [WebSocket protocol](https://pusher.com/docs/channels/library_auth_reference/pusher-websockets-protocol/), [HTTP signatures](https://pusher.com/docs/channels/library_auth_reference/rest-api/), [channel authorization](https://pusher.com/docs/channels/library_auth_reference/auth-signatures/).

## Contributing and license

MIT licensed. See [CONTRIBUTING.md](CONTRIBUTING.md) for setup, test expectations and useful areas to improve, and [SECURITY.md](SECURITY.md) for security reports. Pocket Broadcast is an independent project and is not affiliated with Pusher or Laravel.

App credentials, runtime databases and dependencies are ignored. The npm package is marked `private` to prevent accidental registry publishing; the source is available under the MIT license on GitHub.
