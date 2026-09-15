# Public operations reference

This document explains reusable Horizon operations through a sanitized
reference deployment. Every address, identity, profile, path, and remote name
shown here is synthetic or loopback-only; none is a claim about a live host.
Real operational choices belong in an external private deployment overlay.

## Reference network boundary

The example flow is:

    trusted reverse proxy
            |
            +-- web API on 127.0.0.1:8444
            |
            +-- example gameplay listener on 192.0.2.10:25565
                             |
                             +-- fixed backend 127.0.0.1:25566
                             +-- fixed RCON 127.0.0.1:25575
                             +-- fixed metrics 127.0.0.1:19565

Only the reviewed gameplay listener and trusted proxy are ingress points. The
backend, RCON, metrics, controller socket, and credential files remain outside
browser authority. A name such as `mc.example.com` is a documentation
placeholder.

The checked-in Minecraft profile is one reviewed package fixture. Its mutable
data, immutable release data, backup root, unit, runner, ports, and owner are
fixed by configuration. A request cannot select an executable, path, unit,
endpoint, credential, or alternate profile outside that configuration.

## Console, RCON, and online backup

The command route accepts only a bounded printable command from an
authenticated operator. The controller maps it to the profile's fixed
transport; callers cannot provide a host, port, password path, FIFO, or
executable.

The RCON reference is loopback-only, reads a root-owned mode-0600 runtime
credential, bounds packet and response sizes, and returns generic failures
without command or credential material.

Online application backup is controller-owned:

1. prove eligibility and acquire the durable operation lease;
2. quiesce and flush through fixed typed commands;
3. perform one bounded immutable staging and copy pass;
4. verify staged bytes and manifest metadata;
5. run the fixed cleanup command even when copying fails; and
6. publish catalog metadata only after quiesce and verification pass.

Cleanup failure is fail-closed. An archive is not advertised as verified merely
because bytes were written.

## Capability-scoped wake

A wake proxy is a supervisor, not a Java lifecycle owner. It submits a typed
request to Horizon, waits for a healthy typed status, and cannot launch,
signal, stop, or restart the game process directly.

Capability bindings are fixed by a root-owned issuer. Status and wake scopes,
profile binding, audience, expiry, replay controls, rate budget, cooldown,
transport sizes, and timeouts are independently checked. Request bodies carry
only a request UUID and allowlisted action; they cannot select a profile,
command, URL, credential, path, or process.

A slot conflict becomes `already_active` only after a fresh status proves the
fixed target owns the slot and is starting or running. An unrelated owner,
stale state, failed verification, or an untyped response remains a failure.

## Fenced public relay recovery

The public relay client is a fenced unit, not the game lifecycle. It runs only
while the root-owned arm marker exists, refuses to start when the marker is
absent or has drifted from its expected owner and mode, and is never enabled
at boot. The client carries `Restart=always`, so an unexpected exit -- including
the clean exit a remote relay produces when its control connection drops or
its host reboots -- is restarted by systemd. `RestartSec` paces the retries so
a long remote outage stays a bounded, non-hot loop, and the start rate limit
is disabled so that outage cannot strand the unit in a failed state that
requires an operator to reset it.

Intentional stops stay intentional:

- `systemctl stop`, and `systemctl disable --now` (which stops as well as
  disables), are honored; systemd never restarts a unit after an explicit stop
  request. `systemctl disable` on its own does not stop a running client, and
  because this unit ships no `[Install]` section it has no enablement state to
  change either way.
- Removing the arm marker gates later starts: every subsequent start or
  auto-restart job is skipped because the start condition no longer holds. It
  disarms the relay without terminating a client that is already running.
- Stopping the game backend or its local wake proxy is a separate, intended
  state. The liveness check skips rather than restarting the client whenever
  the local proxy is inactive, and no timer path starts an inactive client.
- The liveness timer keeps its own half-open recovery, ownership proof, arm
  fence, failure threshold, and restart cooldown; it only restarts a client
  that is already active.

## Fixed backup reconciliation

The reconciliation command accepts no caller-supplied profile, remote, prefix,
archive, staging, retention, credential, or prune target. Reviewed
configuration fixes those values and points to a protected runtime credential.

Planning and application are separate:

1. read local catalog/protection state and the fixed remote listing;
2. validate profile, generation, manifest, size, and full digest identity;
3. refuse the complete plan on any mismatch;
4. upload only fixed current local candidates;
5. verify each replacement before prune or catalog mutation;
6. prune only exact allowlisted obsolete objects, never unrelated objects; and
7. publish protected state only after matching local/remote verification.

Repeated plan/apply with unchanged evidence is a no-op. No remote key,
generation, archive size, digest, listing, or backup evidence belongs in this
repository.

## Recovery and verification

- Test installer apply/check and the independent static verifier against a
  disposable alternate root first.
- Validate units and their effective restrictions before copying anything to a
  real host.
- Provision credentials through the target's private secret mechanism.
- Exercise typed health, cancellation, startup reconciliation, backup, restore,
  and rollback paths using disposable data.
- Treat unavailable or stale telemetry as unavailable or stale, never as zero.
- Record real topology, account choices, maintenance procedures, and observed
  evidence only in the external private overlay.

## Schedule operator reference

Editable schedules live in a sibling `schedules.toml` file next to the
configured state database. The operator-facing schedule path is selected by
the private deployment configuration. The root configuration fallback is used
only when that sibling override is absent; an explicitly empty override
disables the legacy fallback. The schedule file remains controller-owned and
is replaced atomically after validation.

This public reference intentionally omits monitoring inventory, migration
evidence, production domains, credentials, live IDs, remote keys, player/world
data, and deployment history.
