# Contributing to Pocket Broadcast

Small, focused contributions are welcome. Start with an issue for a protocol extension or architecture change so its scope and shared-hosting cost can be discussed.

## Development

Use Node 24 or newer with built-in SQLite support and a POSIX shell (Linux/macOS).

```sh
npm ci
npm run check:runtime
npm test
```

The tests create temporary databases and listen only on loopback interfaces. They use synthetic credentials and the official Pusher SDKs and Laravel Echo. No hosting account is required. To try a real installation, follow the README's remote smoke test using a dedicated test app. Use `npm run benchmark` for a reproducible local two-worker latency baseline; document its connection/event parameters when reporting results.

## Pull requests

- Explain the problem and observable behavior after the change.
- Add a regression test for protocol, authorization, cross-worker or reconnect changes. Use real SDK behavior where possible.
- Keep app isolation, signed publishing, private-channel authorization and resource limits intact.
- Keep credentials, private messages, hosting account identifiers and runtime databases out of commits and issue attachments.
- Use CommonJS, four-space indentation, single quotes and strict comparisons. Avoid dependencies that need native compilation on the hosting server.
- Update the README when configuration, compatibility or deployment steps change.

## Useful contributions

- Tests across more Passenger/Plesk hosts and supported Node versions.
- Realistic connection/load measurements with reproducible hardware and hosting limits.
- Failure tests for concurrent processes, disk pressure and worker restarts.
- More protocol conformance tests and clearer diagnostics without secret exposure.

Report what was measured rather than inferring production reliability from a short test. Keep the supported protocol subset explicit. This project intentionally does not promise durable delivery; applications must refresh authoritative state after reconnecting.
