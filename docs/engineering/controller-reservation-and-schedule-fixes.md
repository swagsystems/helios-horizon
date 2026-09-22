# Controller reservation concurrency and schedule fire claims

Feature areas: lifecycle controller, reservation ownership, scheduled switches.

## Reservation lock ordering

Controller transactions acquire the operation file lock in a worker thread,
then execute SQLite and generation callbacks on the owning event loop. Calling
a blocking reservation-store lock method directly from that loop could prevent
the transaction's continuation from running and releasing the lock.

The controller now runs reservation filesystem operations off-loop. This
includes acquisition, renewal, release, rollback transfer, startup
reconciliation, and update-handoff authorization. Database callbacks and
generation reads remain on the event loop.

Cancellation drains the reservation operation before returning. If a cancelled
acquisition or transfer completed successfully, exact-owner cleanup removes
that newly acquired lease before cancellation propagates. A failed acquisition
does not release an existing lease. Repeated cancellation during cleanup still
waits for the writer, and an in-flight renewal finishes before release removes
its lease. Cancellation of a releasing caller is preserved separately from the
intentional cancellation of the renewal task.

## Scheduled switch claims

Scheduled switch execution now consumes only entries whose durable fire claims
were newly accepted in the current tick. Duplicate entries and same-minute
schedule-book reloads no longer bypass an existing `scheduled_fire` marker.
Backup and benchmark claim behavior is unchanged.

## Verification

Regression coverage uses real file locks, reservation writers and controller
job/event writers with temporary state. The deadlock reproduction runs in a
bounded child process so a regression cannot hang the test runner. Coverage
also checks repeated cancellation, exact-owner cleanup, renewal ordering,
duplicate switches and same-minute schedule reloads.

```sh
PYTHONPATH=src python -m pytest -o addopts= -q \
  tests/test_controller_lifecycle_regressions.py \
  tests/test_controller.py tests/test_schedule.py \
  tests/test_slot_state.py tests/test_slot_runner.py tests/test_schedule_config.py
```

Focused result: **181 passed, 4 skipped**.

An independent combined review run applied these changes together with the
reviewed data-lane fixes and ran the non-browser suite: **2,220 passed,
4 skipped, 1 deselected in 150.12 seconds**. The existing 512 MiB quota test was
excluded from that run. This combined result is not a standalone full-suite
result for this branch.

These are source-level results; this change does not deploy or restart the
controller or any game server.
