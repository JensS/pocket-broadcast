# Security

Pocket Broadcast is an early implementation. Use trusted TLS, a separate random secret per app, exact allowed origins and Laravel's authenticated channel authorization. Keep configuration and SQLite data outside the public document root. All applications in one hosting account share that account's security boundary.

For a suspected vulnerability, use the repository's private **Report a vulnerability** option if enabled. Otherwise contact the repository maintainer privately before sharing exploit details. Do not post live secrets, signed production requests, private payloads or hosting access details in public issues.

For a useful report, include affected versions, expected versus actual behavior, impact and a minimal reproduction using synthetic credentials. No response-time or support-window guarantee is currently offered.

Repository maintainers: enable GitHub private vulnerability reporting before publication. Keep dependency updates and security fixes available in the default branch; release/tag a fixed version when a security issue is addressed.
