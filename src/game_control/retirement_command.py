"""Operator CLI for controller-owned local-payload retirement.

The command carries only a root-trusted manifest digest/operation id and a
phase.  It performs the two-phase typed RPC (prepare then confirm) over the
privileged Unix socket and never handles a filesystem path or manifest entry.
"""

from __future__ import annotations

import re
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any
from uuid import uuid4

from .protocol import (
    ConfirmRetirement,
    GetRetirementStatus,
    PrepareRetirement,
    RpcRequest,
    RpcResponse,
    RpcSuccess,
    parse_request_line,
    response_from_json,
)


CONTROL_SOCKET = Path("/run/game-control/control.sock")
_DIGEST = re.compile(r"^[0-9a-f]{64}$")
ACTOR = "cli-retirement"

# Read-only status is cheap; a confirmed prepare re-hashes the whole proposal
# (tens of GB) off the controller event loop, so it needs a bounded, explicit
# operation deadline.  These defaults apply ONLY to this CLI operation and do
# not change any other RPC client's deadlines.
_DEFAULT_TIMEOUT_SECONDS = 30.0
_OPERATION_TIMEOUT_SECONDS = 900.0


class RetirementCommandError(RuntimeError):
    """A safe, operator-actionable CLI refusal."""


def validate_operation_id(value: str) -> str:
    if not isinstance(value, str) or not _DIGEST.fullmatch(value):
        raise ValueError("operation id must be exactly 64 lowercase hexadecimal characters")
    return value


def timeout_for(action: Any) -> float:
    """Per-action client deadline (seconds)."""

    if isinstance(action, (PrepareRetirement, ConfirmRetirement)):
        return _OPERATION_TIMEOUT_SECONDS
    return _DEFAULT_TIMEOUT_SECONDS


def _socket_call(
    request: RpcRequest,
    *,
    socket_path: Path = CONTROL_SOCKET,
    timeout: float | None = None,
) -> RpcResponse:
    import socket as _socket

    deadline = timeout_for(request.action) if timeout is None else timeout
    try:
        with _socket.socket(_socket.AF_UNIX, _socket.SOCK_STREAM) as client:
            client.settimeout(deadline)
            client.connect(str(socket_path))
            client.sendall(request.model_dump_json().encode() + b"\n")
            buffer = b""
            while b"\n" not in buffer:
                chunk = client.recv(65536)
                if not chunk:
                    break
                buffer += chunk
                if len(buffer) > 1024 * 1024:
                    raise RetirementCommandError("control response exceeded its bound")
    except _socket.timeout as exc:
        # Never retry a mutation automatically: the controller may still be
        # completing the phase.  Report a safe, bounded outcome-unknown.
        raise RetirementCommandError(
            f"control service did not respond within {int(deadline)}s; "
            "outcome may be pending — run 'horizon retirement status' before retrying"
        ) from exc
    except OSError as exc:
        raise RetirementCommandError("control service connection failed") from exc
    line = buffer.split(b"\n", 1)[0]
    if not line:
        raise RetirementCommandError("control service returned no response")
    return response_from_json(line)


def _exchange(action: Any, *, call: Callable[[RpcRequest], RpcResponse] | None = None) -> Any:
    request = RpcRequest(request_id=uuid4(), actor=ACTOR, action=action)
    sender = call or _socket_call
    response = sender(request)
    if not isinstance(response, (RpcSuccess,)) or not getattr(response, "ok", False):
        error = getattr(response, "error", None)
        code = getattr(error, "code", "retirement_failed")
        message = getattr(error, "message", "retirement request failed")
        raise RetirementCommandError(f"{getattr(code, 'value', code)}: {message}")
    return response.result


def _phase(operation_id: str, phase: str, *, call: Callable[[RpcRequest], RpcResponse] | None = None) -> dict[str, Any]:
    prepared = _exchange(
        PrepareRetirement(kind="prepare_retirement", operation_id=operation_id, phase=phase),
        call=call,
    )
    confirmation_id = getattr(prepared, "confirmation_id", None)
    if not confirmation_id:
        raise RetirementCommandError("control service did not return a confirmation")
    accepted = _exchange(
        ConfirmRetirement(kind="confirm_retirement", confirmation_id=confirmation_id),
        call=call,
    )
    return {
        "operation_id": operation_id,
        "phase": phase,
        "job_id": getattr(accepted, "job_id", None),
        "state": getattr(accepted, "state", None),
    }


def status(operation_id: str | None = None, *, call: Callable[[RpcRequest], RpcResponse] | None = None) -> dict[str, Any]:
    result = _exchange(
        GetRetirementStatus(kind="get_retirement_status", operation_id=operation_id),
        call=call,
    )
    return result.model_dump(mode="json") if hasattr(result, "model_dump") else dict(result)


def run(argv: Sequence[str], *, call: Callable[[RpcRequest], RpcResponse] | None = None) -> dict[str, Any]:
    """Dispatch one parsed retirement subcommand."""

    values = list(argv)
    if not values:
        raise RetirementCommandError("retirement subcommand is required")
    command, rest = values[0], values[1:]
    if command in {"status", "inspect", "reconcile"}:
        operation_id = rest[0] if rest else None
        if operation_id is not None:
            operation_id = validate_operation_id(operation_id)
        return status(operation_id, call=call)
    if command in {"quarantine", "purge", "rollback"}:
        if len(rest) != 1:
            raise RetirementCommandError("exactly one operation id is required")
        return _phase(validate_operation_id(rest[0]), command, call=call)
    raise RetirementCommandError("retirement subcommand is not registered")


__all__ = ["run", "status", "validate_operation_id", "RetirementCommandError"]
