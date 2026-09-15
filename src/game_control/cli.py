"""Closed, typed operator CLI for Horizon.

The command tree is intentionally declared in source.  It is not a generic
dispatcher: no command, handler, executable, unit, URL, or filesystem policy
is selected by an input string.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from . import backup_command, capability_issue, deployment_verify, journal_evidence, jvm_args, retirement_command, session_revoke, sunlit_update


_UNSAFE_TEXT = re.compile(r"[;&|$`()<>\x00-\x1f]")
def _pending(_args: argparse.Namespace) -> int:
    raise RuntimeError("Horizon CLI handler is not installed")


def _backup(args: argparse.Namespace) -> int:
    return backup_command.reconcile(("--apply",) if args.apply else ())


def _retirement(args: argparse.Namespace) -> int:
    values = [args.retirement_command]
    if getattr(args, "operation_id", None):
        values.append(args.operation_id)
    try:
        result = retirement_command.run(values)
    except (retirement_command.RetirementCommandError, ValueError) as exc:
        print(f"horizon: retirement: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(result, sort_keys=True))
    return 0


def _capability(_args: argparse.Namespace) -> int:
    return capability_issue.issue()


def _deployment(args: argparse.Namespace) -> int:
    values: list[str] = []
    if args.static:
        values.append("--static")
    if args.root is not None:
        values.extend(("--root", str(args.root)))
    if args.relay_state is not None:
        values.extend(("--relay-state", args.relay_state))
    return deployment_verify.main(values)


def _journal(args: argparse.Namespace) -> int:
    if args.journal_command == "capture":
        print(journal_evidence.capture(args.invocation_id))
        return 0
    if args.journal_command == "zero-suppression":
        journal_evidence.verify_zero_suppression(args.invocation_id)
        return 0
    return journal_evidence.finalize()


def _session(_args: argparse.Namespace) -> int:
    return session_revoke.revoke_all()


def _jvm(args: argparse.Namespace) -> int:
    if args.action == "verify":
        return jvm_args.verify()
    if args.action == "activate":
        return jvm_args.activate()
    return jvm_args.rollback()


def _sunlit(args: argparse.Namespace) -> int:
    result = sunlit_update.run(check_only=args.check)
    if result is not None:
        print(result)
    return 0


def _reject_duplicate_options(argv: Sequence[str]) -> None:
    seen: set[str] = set()
    value_options = {"--root", "--relay-state", "--operation-id"}
    index = 0
    while index < len(argv):
        value = argv[index]
        if value.startswith("--"):
            option = value.split("=", 1)[0]
            if option in {"--apply", "--static", "--check", "--root", "--relay-state", "--operation-id"}:
                if option in seen:
                    raise ValueError(f"duplicate option: {option}")
                seen.add(option)
                if option in value_options and "=" not in value:
                    index += 1
                    if index >= len(argv):
                        break
        index += 1


def _reject_unsafe_values(argv: Sequence[str]) -> None:
    for value in argv:
        if _UNSAFE_TEXT.search(value):
            raise ValueError("unsafe command argument")


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="horizon")
    commands = parser.add_subparsers(dest="command", required=True)

    backup = commands.add_parser("backup")
    backup_commands = backup.add_subparsers(dest="backup_command", required=True)
    reconcile = backup_commands.add_parser("reconcile")
    reconcile.add_argument("--apply", action="store_true")
    reconcile.set_defaults(handler=_backup)

    retirement = commands.add_parser("retirement")
    retirement_commands = retirement.add_subparsers(dest="retirement_command", required=True)
    for phase in ("quarantine", "purge", "rollback"):
        phase_parser = retirement_commands.add_parser(phase)
        phase_parser.add_argument("operation_id", type=_operation_id)
        phase_parser.set_defaults(handler=_retirement)
    status = retirement_commands.add_parser("status")
    status.add_argument("--operation-id", type=_operation_id)
    status.set_defaults(handler=_retirement)
    inspect = retirement_commands.add_parser("inspect")
    inspect.set_defaults(handler=_retirement)
    reconcile_retirement = retirement_commands.add_parser("reconcile")
    reconcile_retirement.set_defaults(handler=_retirement)

    capability = commands.add_parser("capability")
    capability_commands = capability.add_subparsers(dest="capability_command", required=True)
    issue = capability_commands.add_parser("issue")
    issue.set_defaults(handler=_capability)

    deployment = commands.add_parser("deployment")
    deployment_commands = deployment.add_subparsers(dest="deployment_command", required=True)
    verify = deployment_commands.add_parser("verify")
    verify.add_argument("--static", action="store_true")
    verify.add_argument("--root", type=Path)
    verify.add_argument("--relay-state", choices=("private", "production"))
    verify.set_defaults(handler=_deployment)

    journal = commands.add_parser("journal")
    journal_commands = journal.add_subparsers(dest="journal_command", required=True)
    capture = journal_commands.add_parser("capture")
    capture.add_argument("invocation_id", type=_invocation_id)
    capture.set_defaults(handler=_journal)
    suppression = journal_commands.add_parser("zero-suppression")
    suppression.add_argument("invocation_id", type=_invocation_id)
    suppression.set_defaults(handler=_journal)
    finalize = journal_commands.add_parser("finalize")
    finalize.set_defaults(handler=_journal)

    session = commands.add_parser("session")
    session_commands = session.add_subparsers(dest="session_command", required=True)
    revoke = session_commands.add_parser("revoke-all")
    revoke.set_defaults(handler=_session)

    tuning = commands.add_parser("tuning")
    tuning_commands = tuning.add_subparsers(dest="tuning_command", required=True)
    jvm = tuning_commands.add_parser("jvm-args")
    jvm.add_argument("action", choices=("verify", "activate", "rollback"))
    jvm.set_defaults(handler=_jvm)

    sunlit = commands.add_parser("sunlit")
    sunlit_commands = sunlit.add_subparsers(dest="sunlit_command", required=True)
    update = sunlit_commands.add_parser("update")
    update.add_argument("--check", action="store_true")
    update.set_defaults(handler=_sunlit)

    return parser


def _invocation_id(value: str) -> str:
    if not re.fullmatch(r"[0-9a-fA-F]{32}", value):
        raise argparse.ArgumentTypeError("invocation ID must be exactly 32 hexadecimal characters")
    return value.lower()


def _operation_id(value: str) -> str:
    if not re.fullmatch(r"[0-9a-f]{64}", value):
        raise argparse.ArgumentTypeError("operation id must be exactly 64 lowercase hexadecimal characters")
    return value


def build_parser() -> argparse.ArgumentParser:
    return _parser()


def main(argv: Sequence[str] | None = None) -> int:
    # Normalize both programmatic and console entry points to the same token
    # list.  Validation must happen before argparse and, more importantly,
    # before any command handler can be selected or invoked.
    values = list(argv) if argv is not None else list(sys.argv[1:])
    try:
        _reject_duplicate_options(values)
        _reject_unsafe_values(values)
        args = _parser().parse_args(values)
    except ValueError as exc:
        raise SystemExit(f"horizon: error: {exc}") from exc
    handler: Any = vars(args).get("handler")
    if handler is None:
        raise SystemExit("horizon: error: command is not registered")
    return int(handler(args))


__all__ = ["build_parser", "main"]
