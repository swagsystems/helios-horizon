# Restore rollback and ZIP release permissions

## Restore publication

Restore records successful displacement and publication renames in its in-memory
rollback lists before syncing the parent directory. If a directory sync raises
after a rename, the error handler can still remove the published generation and
restore every original root. Successful rollback removals and renames also sync
their parent directories.

Rollback cleanup errors are not suppressed. If cleanup, rollback rename, or
rollback sync fails, the durable journal remains available for startup
reconciliation. The journal format and reconciliation decisions are unchanged.
An unsuccessful restore must not be treated as verified recovery until the
original roots have been restored or reconciliation has completed.

## ZIP release files

ZIP extraction normalizes regular output files to mode `0644`. Unix regular-file
metadata may add its execute bits, producing `0644 | (archive_mode & 0111)`.
Set-user-ID, set-group-ID, sticky, and group/world-write bits are never copied from
the ZIP. DOS attributes and Unix attributes without a regular-file type do not
grant execution permission. Directory handling is unchanged.

This permission step occurs before the staged-tree sync and the configured
version command. A checksum-pinned ZIP can therefore provide a directly executed
version probe without losing its Unix executable mode. Symlink and path-traversal
checks remain in place. TAR extraction and raw-payload handling are unchanged.

## Regression coverage

`tests/test_data_audit_regressions.py` covers:

- A real direct version probe from equivalent ZIP and TAR executable fixtures.
- Unix executable/data files, privilege-bit stripping, DOS attributes, and
  missing regular-file metadata.
- ZIP symlink and parent-directory traversal rejection.
- One-shot directory-sync failures after displacement and publication renames.
- Secondary rollback removal, rename, and directory-sync errors, with a retained
  journal and successful later reconciliation.

All fixtures use small temporary files. The local regression archive transport
does not start services or use the network, and extraction quotas are reduced to
byte-sized test limits. These tests establish source behavior, not deployed game
startup or real-world recovery acceptance.
