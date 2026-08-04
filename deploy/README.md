# Single-node deployment

`shard-telemetry.service` is the hardened baseline for the open single-tenant
server. HA, replication, quorum writes, and automatic failover are intentionally
not configured here; those are licensed-distribution features.

Before enabling the unit:

1. Install the release binary as `/usr/local/bin/shard-telemetry-server`.
2. Create the `shard-telemetry` system user and group.
3. Put a random token of at least 16 bytes in `/etc/shard-telemetry/auth-token`, owned
   by `root:shard-telemetry` with mode `0640` or stricter.
4. Edit the tenant, shard count, retention, and capacity flags in the unit.
5. Terminate TLS in a local reverse proxy and expose only the proxy. The native
   listener should remain private unless the surrounding network supplies
   equivalent encryption and identity controls.
6. Run `systemctl daemon-reload && systemctl enable --now shard-telemetry`.

Use `/ready` for readiness and `/metrics` for scraping; both are intentionally
unauthenticated so local supervisors can observe a failed process. Every other
HTTP route requires `Authorization: Bearer <token>`. Stop the unit normally so
SIGTERM can drain ingestion and flush the durable source and index checkpoint.

The data directory contains `FORMAT` and `LOCK`. Never run two processes on one
directory or edit the format marker. An offline backup must include the complete
data and object-store directories from the same stopped instance. Restore them
as a pair to empty directories, retain ownership and modes, and verify startup
on an isolated loopback listener before replacing the active instance.

The current recovery journal and resident embedded indexes are bounded but not
yet object-tier segmented. Capacity planning must include both until the
petabyte-scale tier integration gate documented in `LOKI_COMPATIBILITY.md` is
closed.
