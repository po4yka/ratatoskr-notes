# Security Policy for Ratatoskr Notes

Report vulnerabilities privately. Do not publish Obsidian account passwords, end-to-end encryption
passwords, MFA codes, Sync tokens, device credentials, the notes secrets key, vault content, or
exploit vaults.

Security review is mandatory for: the custody of Obsidian secrets, the systemd template units and
their credentials, the polkit rule, process execution of `ob`, paths and symlinks in a working tree,
the blob root and its read group, the claims that Edge mints, and the deletion of a connection.

Baseline: the Obsidian passwords are stored only as ciphertext and the MFA code is never stored; no
secret enters argv, a log line, an event, an operation report, an error envelope or a metrics
label; secrets reach the login unit only through systemd `LoadCredential=`; `ob` runs as the
dedicated user `ratatoskr-notes-sync` with its state in a 0700 directory; the domain API is
loopback-only and trusts only the claims that Edge mints; a connection of another owner answers
`404`.
