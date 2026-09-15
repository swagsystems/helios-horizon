# Experimental startup estimates

This document describes the opt-in startup estimate track on the current
session deck. It records measured source behavior only. It is not a deployment
record, not a claim about any running host, and not lifecycle authority.

## Visible behavior

When the operator enables **Experimental startup estimate** in Settings, the
dashboard draws one compact left-aligned track below the existing three-stage
session runway while a start is in progress:

```
[ Request accepted ── Starting ── Ready to join ]
[ Estimated progress                17% · about 50s remaining ]
[ green filled track ]
```

- Before five genuine samples exist for the profile and installed version, the
  track stays in a non-numeric learning state and reads
  `Learning startup time…`, with the available recorded count toward five
  successful starts, or the five-start requirement when the count is unavailable.
  The learning state is indeterminate: the track
  omits `aria-valuenow` instead of claiming zero progress. Missing attempt
  elapsed time is treated the same way, so absent metadata never becomes a
  fabricated number.
- With history, the note shows a rolling estimate and the fill advances once
  per second between authoritative status frames. Server-provided elapsed time
  anchors the estimate, so reopening or refreshing a tab mid-start resumes the
  same estimate instead of restarting at zero. Only a new authoritative status
  sample re-anchors; incidental re-renders reuse the anchor, so progress never
  moves backwards between frames.
- The fill is capped below 100 during a start. `100` with the text
  `Ready to join` appears only after the status reports the same authoritative
  readiness criterion the session runway uses: state `running`, health
  `healthy`, required ports ready, and this profile as the current slot owner.
  A `starting` status that already carries healthy/running-looking fields stays
  below 100. The completed run holds `100` while that readiness still holds,
  and clears on stop, failure, wrong ownership, a stale/disconnected status, or
  a different current profile.
- Past the median the note reads `Taking longer than usual…` and the fill stops
  at the cap; there is no negative countdown.
- Missing elapsed time or missing history shows the non-numeric learning state
  described above. Stop, failure, profile change, a stale or disconnected
  status, and a disabled setting hide the track. The authoritative stage
  display is unchanged and stays independent.
- There is no shimmer, loop, or perpetual animation. The fill transition is
  neutralized by the existing reduced-motion rule, and the track uses the
  existing palette tokens.

The setting is browser-local (`localStorage` key `helios-startup-estimate`,
default off). Reads and writes are best-effort: an explicit in-page choice stays
effective when storage writes fail, even if an older opposite value is still
readable. Real localStorage events synchronize the toggle and track across
same-origin tabs; clearing the key resets the default. Unrelated sessionStorage
events do not change the preference. Persistence across a reload still requires
a successful storage write.

Hiding, disconnecting, or resuming the tab invalidates the estimate's cached
sample. A new accepted status is required before the track can return, and the
page must be visible with an active session. Fresh SSE can restore the read-only
estimate when the resume REST request fails; lifecycle action confirmation is
unchanged. Heartbeats and malformed or unrelated status frames cannot restore it.

## Collection and learning

Training is controller-owned and happens only when a genuine start completes.

- The controller keeps the last 25 validated successful starts per profile and
  installed version, requires five samples before reporting a median, and
  budgets old version buckets to four per profile. A version change starts a
  new bucket; an unknown version never reuses a known version's history.
- Only the median is reported. Failed, cancelled, timed-out, rejected,
  idempotent-replay, and no-op starts are never recorded, and malformed
  durations (negative, zero, non-finite, non-numeric, or implausible) are
  rejected rather than clamped.
- Each row stores the duration, completion timestamp, profile, version, and an
  opaque per-attempt run key used for deduplication. No logs, identities,
  paths, or player data are stored.
- The version comes from bounded, freshly read profile metadata (a version file
  read through a size-limited, symlink-rejecting descriptor with a bounded
  wait). A cached status projection is never trusted for training, because it
  can predate a just-applied update; when the version is unknown the start is
  not recorded and the projection stays in learning mode.
- Durations come from monotonic elapsed time around the adapter start and the
  existing readiness probe. Restart re-enters the same start path. A confirmed
  switch start measures from the target's start attempt, excluding source stop
  and slot-wait time; rollback starts during a failed switch are not trained.

## API and persistence

`ProfileStatus.startup_estimate` is an optional typed field on the existing
status contract (`sample_count`, `median_seconds`, `attempt_id`,
`elapsed_seconds`, `version`). It flows through the existing status snapshot
and SSE publisher; there is no new browser polling endpoint, and the field is
absent for producers that do not collect it.

The history lives in the controller's configured state-database connection as
an additive table (`startup_estimate_samples`) created idempotently by
`state_db.ensure_additive_state_tables` at controller construction or on the
record path. The read path (`summary`) is strictly read-only: it never creates
schema, and when the table is unavailable it returns an empty projection.
The table is deliberately not part of
`_STATE_TABLES`: the offline migration utility
(`tools/migrations/state_migrate.py`) snapshots a source database as an exact
version-4 table set, so appending a runtime feature table there would make
every deployed database un-migratable. Ordinary status reads remain read-only;
recording happens on the lifecycle completion path inside the controller's own
state transaction (with an inner savepoint, so an estimate failure can never
commit or roll back unrelated controller state). A schema, write, or read
failure only drops the estimate and leaves the successful lifecycle untouched.

## Limits, rollback, and source-only status

- The estimate is a fit to recent local starts. It does not model mod pack
  changes, storage pressure, or host contention, and it says nothing about
  whether a start will succeed.
- Rolling back the feature means disabling the setting in the browser; the
  additive `startup_estimate_samples` table stays inert because no lifecycle,
  slot, or readiness data depends on it.
- A future schema tool or `tools/migrations` inventory revision should teach
  the migration utility about the additive table before that utility is run
  against a database the controller has already written.
- Source-only: this document and its tests describe the reviewed source shape.
  Nothing here is evidence of a deployed build, and no deployment is implied.
