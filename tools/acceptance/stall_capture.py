#!/usr/bin/env python3
"""Finite, read-only capture runner for timestamped perf diagnostics.

Runs for a bounded wall-clock duration, polling one explicitly supplied
``/api/v1/perf`` endpoint, and appends each capture plus its bounded summary to
an output file. It creates no scheduler or daemon, stops at the deadline, and
never prints a credential it read from a private token file.

Horizon's read API authenticates with the private ``X-Game-Control-Proxy``
header plus the ``X-authentik-username`` actor, so that is the primary mode here.
A plain bearer token remains available as a separate, mutually exclusive mode.
Credentials are attached only to the operator-supplied URL, and any redirect
response is refused so a credential cannot be forwarded to another origin.

The operator supplies endpoint, token file and actor privately; nothing is
discovered or defaulted.

Bounds and retention: one fresh, private (0600, no symlink) output file per
invocation unless ``--append`` is given; the file is capped at 64 MiB
(``MAX_OUTPUT_BYTES``) and the run stops with ``stop_reason: "byte_cap"`` rather
than growing without limit. Each line repeats the full retained window (up to
3 x 256 events, roughly 40 KB), so a long run at a short interval is expected to
be byte-capped; raise ``--interval`` for longer coverage. ``--duration`` and
``--interval`` are validated up front and a window needing more than
``MAX_CAPTURES`` (2880) captures is refused before any file is written.
"""

from __future__ import annotations

import argparse
import fcntl
import json
import math
import os
import stat
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import stall_diagnostics  # noqa: E402

MAX_CAPTURES = 2880
MAX_RESPONSE_BYTES = 1 << 20
MAX_TOKEN_BYTES = 4096
MAX_OUTPUT_BYTES = 64 * 1024 * 1024
MAX_DURATION_SECONDS = 6 * 3600.0
MAX_INTERVAL_SECONDS = 300.0
MAX_TIMEOUT_SECONDS = 60.0
PROXY_HEADER = "X-Game-Control-Proxy"
ACTOR_HEADER = "X-authentik-username"


class RedirectRefused(RuntimeError):
    """Raised instead of following a redirect while credentials are attached."""


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):  # noqa: D102
        raise RedirectRefused(f"redirect refused: {code}")


def _bounded_number(value: str, *, name: str, low: float, high: float) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        raise ValueError(f"{name} must be a finite number")
    if not math.isfinite(number) or not low <= number <= high:
        raise ValueError(f"{name} must be finite and between {low} and {high}")
    return number


def read_token(path: str, *, header: str) -> str | None:
    """Read a private token file with a byte cap; the value is never logged."""

    if not path:
        return None
    try:
        with open(path, encoding="utf-8") as handle:
            token = handle.read(MAX_TOKEN_BYTES + 1)
    except OSError as exc:
        raise ValueError(f"{header} token file is not readable: {exc.__class__.__name__}")
    if len(token) > MAX_TOKEN_BYTES:
        raise ValueError(f"{header} token file exceeds the bounded cap")
    token = token.strip()
    return token or None


def build_request(url: str, *, proxy_token: str | None, actor: str | None,
                  bearer_token: str | None, origin: str | None) -> urllib.request.Request:
    request = urllib.request.Request(url, headers={"Accept": "application/json"})
    if proxy_token:
        request.add_header(PROXY_HEADER, proxy_token)
        if actor:
            request.add_header(ACTOR_HEADER, actor)
    elif bearer_token:
        request.add_header("Authorization", f"Bearer {bearer_token}")
    if origin:
        request.add_header("Origin", origin)
    return request


def capture(url: str, *, proxy_token: str | None = None, actor: str | None = None,
            bearer_token: str | None = None, origin: str | None = None,
            timeout: float = 10.0, opener=None) -> dict:
    request = build_request(url, proxy_token=proxy_token, actor=actor,
                            bearer_token=bearer_token, origin=origin)
    credentials = bool(proxy_token or bearer_token)
    handlers = [_NoRedirect()] if credentials else []
    open_url = opener.open if opener is not None else urllib.request.build_opener(*handlers).open
    with open_url(request, timeout=timeout) as response:
        body = response.read(MAX_RESPONSE_BYTES + 1)
    if len(body) > MAX_RESPONSE_BYTES:
        raise ValueError("perf response exceeds the bounded body cap")
    return stall_diagnostics.load_payload(body)


def _open_output(path: str, *, append: bool) -> tuple[int, int]:
    """Open a private, non-symlink output; never overwrite existing data."""

    flags = os.O_WRONLY | os.O_CREAT | os.O_NOFOLLOW | os.O_NONBLOCK
    flags |= os.O_APPEND if append else os.O_EXCL
    descriptor = os.open(path, flags, 0o600)
    try:
        info = os.fstat(descriptor)
        if (not stat.S_ISREG(info.st_mode) or info.st_nlink != 1
                or info.st_uid != os.geteuid() or stat.S_IMODE(info.st_mode) != 0o600):
            raise ValueError("output must be a private, single-linked regular file")
        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        size = os.lseek(descriptor, 0, os.SEEK_END)
        if size >= MAX_OUTPUT_BYTES:
            raise ValueError("output already exceeds the bounded byte cap")
    except BaseException:
        os.close(descriptor)
        raise
    return descriptor, size


def run(args, *, opener=None, clock=time.monotonic, sleep=time.sleep) -> dict:
    duration = _bounded_number(args.duration, name="duration", low=1.0, high=MAX_DURATION_SECONDS)
    interval = _bounded_number(args.interval, name="interval", low=1.0, high=MAX_INTERVAL_SECONDS)
    timeout = _bounded_number(args.timeout, name="timeout", low=0.1, high=MAX_TIMEOUT_SECONDS)
    captures = int(duration // interval) + 1
    if captures > MAX_CAPTURES:
        raise ValueError(
            f"requested window needs {captures} captures, above the {MAX_CAPTURES} cap; "
            f"increase --interval to at least {math.ceil(duration / MAX_CAPTURES)} s or shorten --duration"
        )
    proxy_token = read_token(args.proxy_token_file, header=PROXY_HEADER)
    bearer_token = read_token(args.token_file, header="Authorization")
    state = stall_diagnostics._load_state(args.state)
    descriptor, bytes_written = _open_output(args.output, append=bool(getattr(args, "append", False)))
    handle = os.fdopen(descriptor, "a", encoding="utf-8")
    started = clock()
    deadline = started + duration
    written = 0
    errors = 0
    stop_reason = "deadline"
    try:
        for _ in range(captures):
            if clock() >= deadline:
                break
            report = None
            try:
                payload = capture(args.url, proxy_token=proxy_token, actor=args.actor,
                                  bearer_token=bearer_token, origin=args.origin,
                                  timeout=timeout, opener=opener)
                report = stall_diagnostics.summarize(payload, state=state)
                line = json.dumps({"captured_at": time.time(), "report": report}, sort_keys=True) + "\n"
            except Exception as exc:  # bounded failure accounting, never the token
                errors += 1
                line = json.dumps({"captured_at": time.time(), "error": type(exc).__name__}) + "\n"
            line_bytes = len(line.encode("utf-8"))
            if os.fstat(handle.fileno()).st_size != bytes_written:
                raise ValueError("output size changed outside this capture")
            if bytes_written + line_bytes > MAX_OUTPUT_BYTES:
                stop_reason = "byte_cap"
                break
            # Apply the same byte ceiling and durability to successes and errors.
            # Output failures are terminal; never retry by writing to a failed file.
            handle.write(line)
            handle.flush()
            os.fsync(handle.fileno())
            bytes_written += line_bytes
            if report is not None:
                written += 1
                # Cursor advances only after the evidence line is durable.
                stall_diagnostics._save_state(args.state, report)
                state = {entry["stream"]: {"instance": entry["instance"],
                                           "next_sequence": entry["next_sequence"]}
                         for entry in report["loss"]}
            remaining = deadline - clock()
            if remaining <= 0:
                break
            sleep(min(interval, remaining))
        else:
            if clock() < deadline:
                stop_reason = "capture_cap"
    finally:
        handle.close()
    elapsed = max(0.0, clock() - started)
    return {"captures": written, "errors": errors, "requested_seconds": duration,
            "elapsed_seconds": round(elapsed, 3), "interval_seconds": interval,
            "stop_reason": stop_reason, "bytes_written": bytes_written,
            "byte_cap": MAX_OUTPUT_BYTES, "capture_cap": MAX_CAPTURES}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Bounded read-only perf capture runner")
    parser.add_argument("--url", required=True, help="explicit /api/v1/perf endpoint")
    parser.add_argument("--proxy-token-file", help="private X-Game-Control-Proxy token file")
    parser.add_argument("--actor", help="X-authentik-username value used with the proxy token")
    parser.add_argument("--token-file", help="private bearer token file (mutually exclusive)")
    parser.add_argument("--origin", help="Origin header the endpoint requires, if any")
    parser.add_argument("--duration", required=True, help="seconds to run (1..21600)")
    parser.add_argument("--interval", default="30", help="seconds between captures (1..300)")
    parser.add_argument("--timeout", default="10", help="per-request timeout (0.1..60)")
    parser.add_argument("--output", required=True, help="JSONL file to append captures and summaries")
    parser.add_argument("--state", help="cursor file reused across captures")
    parser.add_argument("--append", action="store_true",
                        help="append to an existing output file instead of refusing to overwrite it")
    args = parser.parse_args(argv)
    if args.proxy_token_file and args.token_file:
        parser.error("--proxy-token-file and --token-file are mutually exclusive")
    if args.token_file and args.actor:
        parser.error("--actor applies only to the proxy-token mode")
    try:
        summary = run(args)
    except (OSError, ValueError) as exc:
        detail = str(exc)[:200] if isinstance(exc, ValueError) else exc.__class__.__name__
        print(json.dumps({"error": type(exc).__name__, "detail": detail}))
        return 2
    print(json.dumps(summary))
    if summary["captures"] == 0 or summary["stop_reason"] != "deadline":
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
