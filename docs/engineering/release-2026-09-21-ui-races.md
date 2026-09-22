# UI request ordering — 2026-09-21

## User-visible changes

- Late console/log responses retain their own profile's cached lines without
  repainting another server's title, status, controls, or logs. Errors from a
  view the operator has left do not interrupt the current view.
- Schedule add/remove/toggle actions share a saving state. Editing is disabled
  until the response arrives, preventing overlapping full-book writes from
  undoing a preceding edit. Failed saves preserve the draft and restore supported
  controls; unsupported schedule operations remain disabled.
- An older schedule refresh cannot replace the latest saved book or its result
  notice. The saving state persists when navigating between config pages.

## Verification

- Eleven focused browser regressions pass. Nine failed on the unchanged source;
  two preserve existing cancellation and navigation-back cache behavior.
- Full Chromium browser suite: 252 passed.
- JavaScript syntax, whitespace, and public-repository boundary checks passed.

This source-only change affects browser behavior, tests, and this release note.
Backend contracts and conflict handling across separate browser tabs are
unchanged. Deployment and live runtime acceptance are separate checks.
