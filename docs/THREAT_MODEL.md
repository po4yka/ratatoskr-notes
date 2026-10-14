# Notes threat model

## Assets

- The Obsidian account password and the end-to-end encryption password of each `sync` connection.
- The Sync token and the derived end-to-end key that `ob` keeps in plaintext in the client state, `/mnt/nvme/ratatoskr/notes/clients/<id>`.
- The content of every connected vault: the working trees, the blob root and the index.
- The notes secrets key `/etc/ratatoskr/notes-secrets.key`.

## Trust boundaries

| Boundary | Rule |
|---|---|
| Edge to Notes | Notes listens on loopback only and trusts only the claims that Edge mints. A connection of another owner answers `404`. |
| Notes to systemd | Notes reaches systemd only through D-Bus, and one polkit rule allows exactly the three template units. |
| Notes to `ob` | `ob` runs as `ratatoskr-notes-sync`, a user that no other service shares. Secrets reach it only through `LoadCredential=` and standard input. |
| Notes to Knowledge | Knowledge reads note bytes through the read group `ratatoskr-notes-blobs`. Only Notes writes. |

## Threats and controls

| Threat | Control |
|---|---|
| A secret leaks through argv | Passwords go to `ob` on standard input; only the one-time MFA code is in argv. |
| A secret leaks through a log, an event or an error | Secrets are typed so that they do not render; error codes are stable and carry no secret. |
| A database dump exposes the passwords | The passwords are stored only as XChaCha20-Poly1305 ciphertext under the key file. |
| A second channel on one vault doubles every change | The uniqueness of `(owner, vault key)` among the connections that are not removed. |
| A removed connection leaves its client state | Disconnect stops the sync unit and runs the cleanup unit, which deletes the working tree and the client state, and deletes the ciphertext. |

## Residual risk

The server can read the whole vault, also an end-to-end encrypted one. That is inherent in a
copy that Ratatoskr manages. The connect flow states it to the owner before it asks for the
end-to-end password.
