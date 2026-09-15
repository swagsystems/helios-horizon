# Laptop archive backup helpers (`ops/laptop-backup`)

Reusable, transport-agnostic helpers for the Horizon laptop archive tier. They
are deliberately free of deployment paths, host identities, VM/LXC IDs and
credentials: a private, root-owned wrapper supplies those at runtime. See
`docs/operations-example.md` for the documentation-only path namespace.

Nothing here mutates live state. The collector only emits metrics, the retention
helper only proposes a plan, and the recovery helper only downloads into a local
quarantine directory that the operator names.

## `collector.py`

Reads one bounded JSON status document (the sender's local status receipt) from
standard input and writes a Prometheus textfile exposition to standard output.
The private wrapper owns reading the document from the trusted host and
installing the output atomically at the node_exporter textfile path; this module
never reads a path on its own.

```sh
# Documentation-only example; the private wrapper supplies real input/output.
/usr/bin/python3 ops/laptop-backup/collector.py < status.json > laptop_backup.prom
```

Emitted series (fixed names, no free-form labels):

- `helios_laptop_backup_collector_success` and `..._collector_timestamp_seconds`
- `helios_laptop_backup_last_success_timestamp_seconds`
- `helios_laptop_backup_last_attempt_timestamp_seconds`
- `helios_laptop_backup_last_attempt_success`
- `helios_laptop_backup_last_attempt_deferred`
- `helios_laptop_backup_last_attempt_hard_failure`
- `helios_laptop_backup_reason_info{code="<taxonomy>"}`

The only label values are drawn from a fixed taxonomy; raw status reasons are
never echoed. On any parse or validation error the collector emits
`..._collector_success 0` **without** re-emitting a previously fresh success
timestamp, so a broken collector cannot leave a false fresh success behind.

## `retention_plan.py`

Plan-only candidate enumeration. It reports eligibility, exclusions and capacity
pressure and refuses to propose candidates until an explicit policy input is
supplied. It implements no deletion. Remote vault objects referenced by a
preserved manifest can never be candidates, and local payload retirement and
remote vault pruning are reported as separate scopes.

```sh
/usr/bin/python3 ops/laptop-backup/retention_plan.py --input inventory.json
```

Ambiguous or malformed catalog, protection, manifest or ledger state is refused
rather than guessed. The private adapter produces the JSON inventory by reading
the state database read-only; this helper never writes a database.

## `recovery.py`

Verified download of a single vault object into a local quarantine directory. It
binds the fetch to an explicit source digest, size and manifest hash, streams in
bounded chunks into an `O_EXCL` temporary file, recomputes SHA-256, checks zstd
integrity, and publishes with a hard link that never overwrites. The transport
is supplied by the operator; no shell is used and no new SSH authority is
created.

```python
from recovery import SourceBinding, CommandTransport, download_to_quarantine

transport = CommandTransport(["/usr/local/libexec/horizon-example-fetch", "{digest}"])
binding = SourceBinding(digest="0" * 64, size_bytes=1, manifest_sha256="1" * 64)
download_to_quarantine(transport, binding, "/var/backups/example-quarantine")
```

Rehydration and ledger reconciliation are **not** implemented here. The
controller owns them; the local retirement ledger treats a purged payload as
terminal.

## Tests

```sh
python3 -m pytest tests/test_laptop_backup_collector.py \
  tests/test_laptop_backup_retention_plan.py tests/test_laptop_backup_recovery.py
```

Prometheus rule fixtures live in `rules/` and are intended for `promtool test`.
