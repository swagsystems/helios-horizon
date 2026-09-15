from __future__ import annotations

from pathlib import Path
import os
from types import SimpleNamespace
import subprocess
import sys

import pytest

from tools.quality import check_public_boundary as boundary


ROOT = Path(__file__).resolve().parents[1]


def _repository(tmp_path: Path, files: dict[str, str | bytes]) -> Path:
    root = tmp_path / "repository"
    root.mkdir()
    subprocess.run(["git", "init", "-q", str(root)], check=True)
    for relative, content in files.items():
        path = root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        if isinstance(content, bytes):
            path.write_bytes(content)
        else:
            path.write_text(content, encoding="utf-8")
    subprocess.run(["git", "-C", str(root), "add", "."], check=True)
    return root


def _categories(root: Path) -> set[tuple[str, int, str]]:
    return {(item.path, item.line, item.category) for item in boundary.scan_repository(root)}


_BROWSER_PREIMAGE_LINE = (
    'ARTIFACTS = Path(os.environ.get("HORIZON_UI_ARTIFACTS", '
    '"/root/workspace-example/ui-artifacts"))'
)


@pytest.mark.parametrize(
    "literal",
    ["/root/dev/artifacts", "/home/dev/artifacts", "/Users/dev/artifacts"],
)
def test_browser_home_paths_are_rejected(tmp_path: Path, literal: str) -> None:
    root = _repository(
        tmp_path, {"tests/browser/test_widget.py": f'OUT = "{literal}/x"\n'}
    )
    assert ("tests/browser/test_widget.py", 1, "browser-local-path") in _categories(root)


def test_browser_env_default_developer_home_literal_is_rejected(tmp_path: Path) -> None:
    # The exact shape of the confirmed CI regression: a home path embedded as
    # the default value of an environment lookup.
    root = _repository(
        tmp_path, {"tests/browser/test_update_and_history.py": _BROWSER_PREIMAGE_LINE + "\n"}
    )
    assert (
        "tests/browser/test_update_and_history.py",
        1,
        "browser-local-path",
    ) in _categories(root)


def test_browser_line_finding_flags_preimage_without_matched_content(tmp_path: Path) -> None:
    findings = boundary._line_findings(
        "tests/browser/test_update_and_history.py", 18, _BROWSER_PREIMAGE_LINE
    )
    assert boundary.Finding(
        "tests/browser/test_update_and_history.py", 18, "browser-local-path"
    ) in findings
    # Safe format only: path, line and category; no matched content.
    rendered = sorted(item.render() for item in findings)
    assert "tests/browser/test_update_and_history.py:18:browser-local-path" in rendered
    assert all("workspace-example" not in item for item in rendered)


def test_browser_portable_paths_are_allowed(tmp_path: Path) -> None:
    root = _repository(
        tmp_path,
        {
            "tests/browser/test_widget.py": (
                'OUT = "artifacts/out"\n'
                'CACHE = tmp_path / "cache"\n'
                'SERVED = "/opt/game-control/web/app.js"\n'
                'HOME_LABEL = "developer-home"\n'
            )
        },
    )
    assert _categories(root) == set()


def test_non_browser_home_path_sentinel_is_not_flagged(tmp_path: Path) -> None:
    # The deliberate /root/... sentinel outside the browser surface stays as-is.
    root = _repository(
        tmp_path, {"tests/test_maintenance_process.py": 'SENTINEL = "/root/secret"\n'}
    )
    assert _categories(root) == set()


def test_clean_tracked_reference_uses_documentation_networks_and_example_namespaces(tmp_path: Path) -> None:
    root = _repository(
        tmp_path,
        {
            "README.md": "https://github.com/swagsystems/helios-horizon\nhttp://127.0.0.1:8444\n",
            "docs/reference.md": "mc.example.com 192.0.2.10 198.51.100.20 203.0.113.30\n",
            "config/examples/profile.toml": (
                'config = "/etc/horizon-example/profiles.d/server.toml"\n'
                'state = "/var/lib/horizon-example/state.db"\n'
                'run = "/run/horizon-example/control.sock"\n'
                'data = "/srv/example-minecraft/world"\n'
                'release = "/opt/example-minecraft/releases/v1"\n'
                'backup = "/var/backups/example-minecraft"\n'
                'helper = "/usr/local/libexec/horizon-example-wake"\n'
            ),
        },
    )

    assert boundary.scan_repository(root) == ()


@pytest.mark.parametrize(
    ("value", "category"),
    [
        (".".join(("games", "heliosorbit", "space")), "private-domain"),
        (".".join(("10", "1", "2", "3")), "private-address"),
        (".".join(("172", "31", "2", "3")), "private-address"),
        (".".join(("192", "168", "2", "3")), "private-address"),
        (".".join(("100", "100", "2", "3")), "private-address"),
        (".".join(("server", "internal")), "private-dns"),
        (".".join(("server", "internal", "")), "private-dns"),
    ],
)
def test_global_private_topology_is_refused_without_echoing_content(
    tmp_path: Path, value: str, category: str
) -> None:
    root = _repository(tmp_path, {"src/example.py": f'VALUE = "{value}"\n'})

    findings = boundary.scan_repository(root)

    assert findings == (boundary.Finding("src/example.py", 1, category),)
    assert value not in findings[0].render()


def test_example_absolute_path_must_use_the_explicit_namespace(tmp_path: Path) -> None:
    root = _repository(
        tmp_path,
        {"config/examples/profile.toml": 'state = "/var/lib/game-control/state.db"\n'},
    )

    assert _categories(root) == {("config/examples/profile.toml", 1, "example-path-namespace")}


@pytest.mark.parametrize(
    "value",
    (
        "/srv/example-minecraft/../../private",
        "/etc/horizon-example/../private",
        "file:///etc/horizon-example/profile.toml",
    ),
)
def test_example_path_cannot_escape_or_bypass_the_namespace(tmp_path: Path, value: str) -> None:
    root = _repository(tmp_path, {"config/examples/profile.toml": f'path = "{value}"\n'})

    assert _categories(root) == {("config/examples/profile.toml", 1, "example-path-namespace")}


def test_non_dns_programming_tokens_are_narrowly_exempt(tmp_path: Path) -> None:
    enum_token = "BackupDestination" + "." + "LOCAL"
    property_token = "user" + "." + "home"
    root = _repository(tmp_path, {"src/example.py": f"{enum_token}\n{property_token}\n"})

    assert boundary.scan_repository(root) == ()


def test_public_name_below_internal_label_is_not_private_dns(tmp_path: Path) -> None:
    root = _repository(tmp_path, {"README.md": "server.internal.example\n"})

    assert boundary.scan_repository(root) == ()


def test_untracked_content_is_not_scanned(tmp_path: Path) -> None:
    root = _repository(tmp_path, {"README.md": "example.com\n"})
    private_address = ".".join(("10", "2", "3", "4"))
    (root / "untracked.txt").write_text(private_address, encoding="utf-8")

    assert boundary.scan_repository(root) == ()


def test_undecodable_tracked_text_fails_closed_without_content(tmp_path: Path) -> None:
    root = _repository(tmp_path, {"README.md": b"\xff\xfe\x00"})

    with pytest.raises(boundary.ScanError, match="tracked-text-unreadable:README.md"):
        boundary.scan_repository(root)


def test_known_binary_asset_is_skipped(tmp_path: Path) -> None:
    root = _repository(tmp_path, {"docs/diagram.png": b"\xff\xfe\x00"})

    assert boundary.scan_repository(root) == ()


def test_binary_suffix_does_not_exempt_a_replaced_symlink(tmp_path: Path) -> None:
    root = _repository(tmp_path, {"docs/diagram.png": "tracked\n", "target.txt": "safe\n"})
    asset = root / "docs/diagram.png"
    asset.unlink()
    asset.symlink_to(root / "target.txt")

    with pytest.raises(boundary.ScanError, match="tracked-file-not-regular:docs/diagram.png"):
        boundary.scan_repository(root)


def test_binary_suffix_does_not_exempt_a_replaced_fifo(tmp_path: Path) -> None:
    root = _repository(tmp_path, {"docs/archive.jar": "tracked\n"})
    asset = root / "docs/archive.jar"
    asset.unlink()
    os.mkfifo(asset)

    with pytest.raises(boundary.ScanError, match="tracked-file-not-regular:docs/archive.jar"):
        boundary.scan_repository(root)


def test_binary_suffix_does_not_exempt_a_device_shape(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _repository(tmp_path, {"docs/diagram.png": "tracked\n"})
    asset = root / "docs/diagram.png"
    original_lstat = Path.lstat

    def fake_lstat(path: Path):
        if path == asset:
            return SimpleNamespace(st_mode=boundary.stat.S_IFCHR | 0o600)
        return original_lstat(path)

    monkeypatch.setattr(Path, "lstat", fake_lstat)
    with pytest.raises(boundary.ScanError, match="tracked-file-not-regular:docs/diagram.png"):
        boundary.scan_repository(root)


def test_tracked_symlink_fails_closed(tmp_path: Path) -> None:
    root = _repository(tmp_path, {"target.txt": "safe\n"})
    link = root / "tracked-link"
    link.symlink_to("target.txt")
    subprocess.run(["git", "-C", str(root), "add", "tracked-link"], check=True)

    with pytest.raises(boundary.ScanError, match="tracked-file-not-regular:tracked-link"):
        boundary.scan_repository(root)


def test_path_replacement_between_lstat_and_open_fails_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _repository(tmp_path, {"README.md": "safe\n"})
    target = root / "README.md"
    replacement = root / "replacement"
    replacement.write_text("also safe\n", encoding="utf-8")
    original_open = os.open
    replaced = False

    def swapping_open(path: str | os.PathLike[str], flags: int, *args: object) -> int:
        nonlocal replaced
        if Path(path) == target and not replaced:
            replaced = True
            replacement.replace(target)
        return original_open(path, flags, *args)

    monkeypatch.setattr(os, "open", swapping_open)
    with pytest.raises(boundary.ScanError, match="tracked-file-changed:README.md"):
        boundary.scan_repository(root)


def test_fifo_replacement_between_lstat_and_open_fails_without_blocking(tmp_path: Path) -> None:
    root = _repository(tmp_path, {"README.md": "safe\n"})
    script = r"""
from pathlib import Path
import os
import sys

from tools.quality import check_public_boundary as boundary

root = Path(sys.argv[1])
target = root / "README.md"
original_open = boundary.os.open
replaced = False

def swapping_open(path, flags, *args):
    global replaced
    if Path(path) == target and not replaced:
        replaced = True
        target.unlink()
        os.mkfifo(target)
    return original_open(path, flags, *args)

boundary.os.open = swapping_open
try:
    boundary.scan_repository(root)
except boundary.ScanError as exc:
    if str(exc) == "tracked-file-changed:README.md":
        raise SystemExit(0)
    raise
raise SystemExit("scanner accepted a FIFO replacement")
"""

    result = subprocess.run(
        [sys.executable, "-c", script, str(root)],
        cwd=ROOT,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=3,
    )

    assert result.returncode == 0, result.stderr


def test_diagnostic_unsafe_tracked_path_fails_closed(tmp_path: Path) -> None:
    root = _repository(tmp_path, {"unsafe:name.txt": "safe\n"})

    with pytest.raises(boundary.ScanError, match="unsafe-tracked-path"):
        boundary.scan_repository(root)


def test_policy_validation_rejects_malformed_allowlist(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(boundary, "NON_DNS_TOKENS", frozenset({"bad\nentry"}))

    with pytest.raises(boundary.ScanError, match="invalid-dns-allowlist"):
        boundary._validate_policy()


def test_cli_requires_an_explicit_repository_root() -> None:
    with pytest.raises(SystemExit) as exc_info:
        boundary.main([])

    assert exc_info.value.code == 2


def test_cli_diagnostics_contain_only_path_line_and_category(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    private_address = ".".join(("10", "2", "3", "4"))
    root = _repository(tmp_path, {"README.md": f"endpoint={private_address}\n"})

    assert boundary.main(["--root", str(root)]) == 1

    captured = capsys.readouterr()
    assert captured.out == "README.md:1:private-address\n"
    assert private_address not in captured.out
    assert captured.err == ""


def test_quality_gates_invoke_the_scanner() -> None:
    command = "tools/quality/check_public_boundary.py --root"

    assert command in (ROOT / "scripts/check.sh").read_text(encoding="utf-8")
    assert command in (ROOT / ".github/workflows/ci.yml").read_text(encoding="utf-8")
