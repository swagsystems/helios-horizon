# Horizon security and lifecycle invariants

This document is the architectural contract for the reviewed Horizon source
shape. It describes authority, truth, persistence, lifecycle, failure, and
recovery boundaries. It is not an audit log, deployment record, or claim about
any particular host.

## Scope and current boundary

Horizon is a fixed-profile, single-slot controller. The web tier is
unprivileged and translates authenticated requests into typed actions. The
root controller is the authority for lifecycle and maintenance admission;
game adapters and domain services execute only bounded, admitted work.

The active packaged topology is the reviewed set of configured profiles. The
`ProfileId` enum also contains historical or candidate identifiers; its full
enumeration is not a claim that every identifier is installed or active.

Add Server, dynamic instances, provider or marketplace acquisition, compiled
templates, and private deployment-overlay extraction are future blueprint
work. They are not current browser capabilities, RPC inputs, or runtime
owners.

## Authority and privilege map

| Boundary | Owns or decides | Must not own or accept |
| --- | --- | --- |
| Root `Controller`, `StateDatabase`, and `ReservationStore` | Typed lifecycle and maintenance admission, jobs, idempotency, generations, leases, publication fences, and root wake evidence | Browser paths, commands, units, endpoints, credentials, or web-database authority |
| Web API/UI and web DB | Proxy/session authentication, CSRF and origin checks, capability grants, replay/rate/cooldown state, HTTP-facing audit, and typed translation | Process control, root state, worlds, backups, or root wake assertions |
| Domain services | Bounded adapter, archive, restore, update, and benchmark work after controller admission | Lease acquisition or caller-selected resources |
| Telemetry and alert runtimes | Disposable observations, explicit unavailable/inactive values, policy evaluation, and bounded notification delivery | Lifecycle truth or fabricated zero/healthy values |
| Fixed runners, console, RCON, and FIFO boundaries | Reviewed profile-specific process and console transport | Arbitrary executables, units, FIFOs, RCON endpoints, or secrets from input |
| Installer and verifier | Declared static package policy and independent target inspection | Treating installer output or a runtime manifest as live proof |

Direct slot runners may act only with a matching live root reservation. They do
not rewrite the root reservation file. Capability callers use a separate,
typed status/wake boundary and cannot reach the operator mutation surface.

## Persistence ownership and truth

The stores have separate owners and meanings:

- The root state DB contains jobs, events and audit records, idempotency and
  confirmation state, notification rules, benchmark history, player-session
  summaries, and legacy metric samples. `StateDatabase` is a root-owned
  writer, opened only at its approved path and closed on its owning thread.
- The web DB contains web sessions, CSRF state, and capability token,
  replay, rate, cooldown, and capability-audit state. The root reader does not
  open the web DB.
- The telemetry DB contains bounded disposable observations. It is not
  lifecycle authority and does not replace root job or reservation state.
- Reservation JSON and the operation lock provide cross-process ownership.
  The typed reservation records and checks exact profile, request/operation ID,
  state generation, controller process identity, and bounded expiry before a
  worker may publish. The root jobs ledger records the operation name; the
  reservation record has no separate operation-name field.
- Staged release, restore, backup, and domain journals are recovery evidence;
  they are not browser-selected paths or lifecycle authority.

File-backed history and telemetry reads use approved, read-only worker-local
SQLite connections. Writer connections remain thread-affine. Typed root-state
readers and safety gates represent a missing, malformed, unreadable, or
conflicting authoritative store as unavailable and fail closed. The
compatibility status path is weaker: `RootActiveJobsReader.__call__()` maps an
unavailable jobs read to `None`, so `derive_state()` can still project
`STOPPED` when the process is not alive and no conflicting slot owner is
reported. That projection limitation is documented below and is not safety
evidence.

## Lifecycle and health states

The public lifecycle states are deliberately finite:

```text
initializing -> stopped -> starting -> running -> stopping -> stopped
                         |             |          |
                         +-----------> failed <---+
```

`ObservedState` exposes `stopped`, `starting`, `running`, `stopping`,
`failed`, and `blocked`; it has no public `unknown` member. `HealthState`
does expose `unknown`, which is the correct representation for unavailable
health or observation evidence. A conceptual unavailable state must not be
serialized as a new lifecycle enum.

The current status implementation has two known compatibility limitations:
an adapter observation error can derive `STOPPED` in some paths, and an
unavailable root-jobs read is mapped to no active job before state derivation,
which can also derive `STOPPED` when the process is not alive and no
conflicting owner is present. Therefore the stronger rule “unavailable
observation never creates a stopped transition” is a target invariant, not a
claim that this exact tree already satisfies it.
Cached status is a UI/operator projection and cannot create a lifecycle
transition or satisfy a safety gate.

Startup reconciliation of currently implemented interrupted jobs, benchmark
rows, reservations, and backup recovery precedes mutation admission. Update
journal startup reconciliation is future: this tree has no
`UpdateService.reconcile_startup()` owner.

## Maintenance jobs and leases

Durable maintenance follows:

```text
none -> accepted -> running -> succeeded
                         \-> failed
                         \-> deferred
```

The API retains its stable `accepted`, `running`, `succeeded`, `failed`, and
`cancelled` response vocabulary. A scheduled backup may persist `deferred`
when its safety conditions are not met. Cancellation or lease loss becomes a
terminal durable outcome only after the worker and its cleanup have drained.

The lease protocol is:

```text
absent -> reserve under operation lock -> renew/assert -> release
```

The reservation records exact profile, request/operation ID, state generation,
controller process identity, and bounded expiry; the root jobs ledger carries
the operation name separately. Renewal loss stops further publication, drains
any worker that can still mutate, records the safe outcome, and releases only
after that drain. Scheduled and manual backup use the same controller lease
authority.

A bounded handoff lets the manual Sunlit update run its pre-update protected
backup under the update reservation it already holds, without acquiring a
second lease. The updater mints a random capability per reservation, stores
only its SHA256 in the reservation, and delivers it to the helper over a
private stdin pipe (never argv, environment, logs, or durable request state).
The controller accepts the capability only for the Sunlit protected
`horizon-b2` backup and revalidates the exact live operation/generation/PID
identity at every publication fence, so it is not a release-and-reacquire and
actor name is never the authorization. A completed backup from an earlier run
is reused only when it is bound to the same continuously-held reservation;
otherwise the updater fails closed rather than promoting on stale evidence.

## Status truthfulness and benchmark admission

Fresh status and slot observations are evidence; a cold cached projection is
not. Status snapshots retain observation time, generation, owner/job, health,
player count, and telemetry values without converting unavailable values into
healthy or zero values.

Benchmark admission requires fresh typed status and slot evidence, root wake
evidence, no conflicting root operation or session, the approved quiet period,
storage and UPS checks, maintenance-window policy, and rollback/public-wake
policy. Driver output is immutable provenance and evidence, not benchmark
policy. `RootWakeSafetyEvidence` reads only root-owned active-job and
reservation evidence and fails closed when that evidence is unavailable.

## Failure and recovery contract

Public failures use bounded typed error codes and redacted details. Mutation
idempotency is claimed before side effects; ambiguous pending work is not
silently replayed. Destructive operations use actor- and operation-bound
prepare/confirm state.

Backup quiesce follows the save-off, flush, copy/verify, save-on sequence, with
save-on attempted from failure cleanup. Restore stages and validates data,
records a journal, and retains rollback handling. Release updates require a
trusted digest, bounded archive extraction, atomic pointer publication, and
fsync of files and directories. Benchmark preflight validates safety before
insertion, freezes provenance, and compares execution-time evidence.

Cancellation and shutdown drain accepted work before releasing its lease or
closing a writer. A failed stage must not erase the last known safe
publication. Unavailable evidence is visible as unavailable, blocked, or a
typed failure rather than being presented as green.

## Telemetry, alert, and close ownership

Modern telemetry has one runtime owner. `TelemetryRuntime` owns its sampler,
persistent RCON, and telemetry database references when marked owned;
`TelemetryCollector` receives values for collection and does not close them.
The runtime stops intake, drains its sampler/cycle and collector work, drains
the telemetry queue, and closes owned resources only after the drain.

`AlertRuntime` owns its bounded delivery tasks and does not close
`NotificationService`. `HistoryQueryService` owns its bounded query executor.
`Controller.aclose()` owns controller background worker handles, consumes
terminal results, cancels and drains workers, and lets lease cleanup finish.

The typed `ServiceContainer` implements the owner ledger and ordered hooks:

```text
Controller.aclose()
  -> owned TelemetryRuntime
  -> owned AlertRuntime
  -> owned legacy TPS sampler
  -> owned Crafty adapters and UpdateServices
  -> owned NotificationService
  -> owned HistoryQueryService
  -> owned StateDatabase (last)
```

Only `ResourceRef.owned(value)` grants a typed resource close authority;
borrowed refs remain usable and open. Shared typed adapters are closed once.
Each stage is attempted after an ordinary error, with the first ordinary
error retained. Caller cancellation is drained through all stages and
re-raised afterward; failed ledgers remain retryable. The synchronous state
database close stays on its owning thread.

Production construction is transactional. `build_controller_assembly()` uses
the provisional owner ledger to acquire the root state, adapters, telemetry,
notifications, updates, history, and typed service seams; it constructs the
`ServiceContainer`, binds the private container slot once, and publishes the
`_RootAssembly` only after finalization. A construction failure drains every
acquired owned resource in dependency order while borrowed resources remain
open. The synchronous `build_controller()` entry point is a compatibility
wrapper that rejects nested event-loop use.

`slotd_main.serve()` owns every supervisor task it creates. Its outer cleanup
envelope cancels, drains, and observes those tasks, closes the RPC server, and
then closes the finalized assembly exactly once. `ServiceSeams.close()` and
`.aclose()` are compatibility delegates to that finalized container owner;
they do not retain an independent collector or database close path. The
composition root and supervisor ownership are current source invariants, not
pending integration work. This remains a source contract only and does not
claim live deployment or activation.

## Explicit tick-source matrix

Legacy TPS mode is an explicit root configuration choice, either `disabled`
or `enabled`; exporter presence never implicitly selects legacy mode. The
effective source is singular:

| Legacy mode | Exporter binding | Effective tick source | Legacy sampler/task |
| --- | --- | --- | --- |
| disabled | absent | none | none |
| disabled | present | exporter only | none |
| enabled | absent | none | legacy sampler only |
| enabled | present | legacy override; no modern tick source | legacy sampler only |

The explicit legacy override prevents duplicate tick cadence and storage
owners. Invalid or missing mode is rejected where required, with the
compatibility default applied only at the root configuration boundary.

## Deployment integrity boundary

`src/game_control/deployment_manifest.py` is the single typed, frozen,
stdlib-only declaration for the package's static and runtime projections. The
schema-1 manifest currently declares three fixed profiles, 65 static files,
50 directories, 69 runtime sources, and 138 projected runtime files, together
with the reviewed symlink, owned namespaces, retired paths, secret metadata,
database roles, runtime-manifest metadata, and relay-mode expectations. Its
constructor validates record types, modes, paths, duplicate/parent
collisions, and immutable collections; source validation requires regular
single-link package files.

`ops/install.py` and `scripts/verify-deployed.py` both load this canonical
module from their own location-derived package root. They do not load a
manifest from the inspected target, current working directory, or runtime
manifest. The installer projects the same declarations into staged or live
targets, including deterministic staged ownership. The read-only verifier
checks declared file/directory/link metadata, namespace contents, retired
artifacts, runtime-manifest integrity, and directory child/link-count policy;
target-side manifest tampering cannot change static expectations. The
manifest and static verifier establish package/target policy, not live proof:
independent probes for services, listeners, cgroups, authentication,
database integrity, relay, and sockets remain necessary. Package-safety
preflight and the allowance for unrelated systemd units remain explicit, and
no live deployment or activation is claimed here.

## Future blueprint boundary

Future design may introduce `ServerInstanceId`, `GameKind`, and `TemplateId`,
compiled profile artifacts, provider acquisition, lockfile and provenance
checks, and staged publication. Those names do not authorize dynamic browser
profile selection, arbitrary paths or commands, a wider public RPC, or a live
marketplace/provider implementation.

The current contract remains the fixed `ProfileId` boundary, reviewed active
profile topology, one-slot reservation, typed controller actions, and
root-owned process and persistence paths. Update-journal startup
reconciliation (no `UpdateService.reconcile_startup()` exists here), dynamic
instances, template compilation, provider acquisition, private deployment
overlay separation, and migration of the managed-tuning data path remain
future work.

## Verification ledger

The claims above are anchored to semantic owners and existing tests rather
than mutable line numbers:

| Invariant | Source anchors | Verification suites |
| --- | --- | --- |
| Root-only lifecycle, maintenance, and wake authority | `src/game_control/controller.py`, `slot.py`, `root_state.py`, `capability_evidence.py` | `tests/test_controller.py`, `tests/test_slot_state.py`, `tests/test_slot_runner.py`, `tests/test_stop_fencing.py`, `tests/test_capability_evidence.py` |
| Web/session/capability separation | `src/game_control/web_main.py`, `web_db.py`, `capability.py`, `auth.py` | `tests/test_auth.py`, `tests/test_api_webtier.py`, `tests/test_capability.py`, `tests/test_capability_retention.py` |
| Persistence and thread ownership | `state_db.py`, `web_db.py`, `telemetry_db.py`, `history_queries.py`, `session_store.py`, `stats_queries.py` | `tests/test_db.py`, `tests/test_state_db_stats.py`, `tests/migrations/test_state_migrate.py`, `tests/test_history_queries.py`, `tests/test_telemetry_db.py`, `tests/migrations/test_telemetry_migration.py` |
| Lifecycle and health truth | `models.py`, `health.py`, `status.py`, `protocol.py` | `tests/test_status.py`, `tests/test_health.py`, `tests/test_status_stats.py`, `tests/test_capability.py` |
| Lease, generation, drain, and recovery | `controller.py`, `slot.py`, `backups.py`, `backup_reconcile.py` | `tests/test_controller.py`, `tests/test_slot_state.py`, `tests/test_slot_runner.py`, `tests/test_schedule.py`, `tests/test_backups.py`, `tests/test_backup_reconcile.py` |
| Benchmark safety and frozen provenance | `benchmark_safety.py`, `driver_preflight.py`, `benchmarks.py` | `tests/test_benchmark_safety.py`, `tests/test_driver_preflight.py`, `tests/test_benchmarks.py` |
| Update, restore, and atomic publication | `updates.py`, `backups.py`, `controller.py` | `tests/test_updates.py`, `tests/test_restore.py`, `tests/test_sunlit_promote.py`, `tests/test_controller.py` |
| Telemetry, alerts, history, and typed close ownership | `runtime/telemetry.py`, `runtime/alerts.py`, `history_queries.py`, `service_container.py`, `notifications.py`, `tps.py` | `tests/test_runtime_telemetry.py`, `tests/test_runtime_alerts.py`, `tests/test_history_queries.py`, `tests/test_service_container.py`, `tests/test_notifications.py`, `tests/test_tps.py` |
| Transactional assembly, supervisor ownership, and startup reconciliation | `slotd_main.py`, `service_wiring.py`, `service_container.py` | `tests/test_slotd_main.py`, `tests/test_service_wiring.py`, `tests/test_service_container.py` |
| Typed deployment manifest and static/live separation | `deployment_manifest.py`, `ops/install.py`, `scripts/verify-deployed.py` | `tests/test_deployment_manifest.py`, `tests/test_packaging.py`, `tests/test_verify_deployed.py`; live deployment remains outside this source contract |

## Glossary

- **Authority:** the component allowed to decide or mutate a class of state.
- **Projection:** a derived view for operators or callers that is not safety
  evidence and cannot create a lifecycle transition.
- **Lease:** a bounded, exact reservation proving one operation owns the slot.
- **Generation:** the root-state version used to fence stale operations.
- **Evidence:** a typed observation with provenance and availability semantics
  that a safety gate may consume.
- **Unavailable:** a typed reader or safety gate could not safely read evidence
  or a store and must fail closed; the compatibility status projection may
  still show `stopped` for an unavailable root-jobs read, as documented above.
- **Deferred:** a durable operation was intentionally not executed because a
  safety or scheduling condition was not met.
- **Terminal:** a job outcome that cannot continue; cancellation and lease
  loss become terminal after accepted worker cleanup has drained.
