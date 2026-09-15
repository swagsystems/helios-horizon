# Usability and confirmation safety — 2026-09-15

## User-visible changes

- Cancel and close controls dismiss dialogs without submitting server actions
  or starting console downloads. Confirmation handlers check the intended button
  and typed name again; dismissing a pending preparation prevents its later
  confirmation. Already accepted server work is not undone by closing a dialog.
- Switch and restore dialogs identify the running server, selected target and
  backup owner, and explain world-data consequences. With no active server,
  confirming a target uses the normal guarded Start path.
- Scheduled automations name their operation and timezone. New profile switches
  have a next-run preview. Schedule reads and edits preserve backup and benchmark
  policy fields. Settings and the command palette link to the editor.
- Unconfigured server entries are named and have no usable lifecycle controls;
  they are excluded from action and notification destination lists.
- The recorder table pages through the selected history window and filters
  active samples. Coverage distinguishes the chart from filtered rows. Player
  totals name their period, and Player activity opens the player section.
  Cached history retains its window identity through failed requests and retries;
  resizing or changing comparisons cannot relabel another window's data.
- Incident details show recorded occurrences and the later matching success
  behind a resolved badge. The badge reflects a bounded audit record.
- Update-check results remain inline during navigation. Late responses cannot
  open an update dialog over another profile. Readiness links to the startup
  estimate preference, and join help uses the current profile and version.
- Long version and automation labels and empty history messages wrap on phones.

The existing themes, green readiness treatment and controller lifecycle rules
remain. This release does not change game versions, world data, tunnel policy,
or retention. Gameplay stall attribution remains a separate diagnostic task.

## Verification

- Python 3.11 backend suite: 2,195 passed, 4 skipped.
- Chromium browser suite: 241 passed, including phone-width layout checks.
- JavaScript syntax, Python compilation, whitespace and public-repository
  boundary checks passed.
- Three independent review passes identified additional edge cases; the fixes
  have focused synthetic browser and API-contract regressions.

Deployment updates the web assets and the schedule read projection in both
source and installed Python modules, with matching manifest and wheel metadata.
Controller and web services must restart to load the projection. Game and tunnel
services are outside this release's restart scope. Runtime acceptance is a
separate deployment check; source tests do not establish live acceptance.
