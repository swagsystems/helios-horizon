from __future__ import annotations

import hashlib
import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

import pytest

OPS = Path(__file__).resolve().parents[1] / "ops" / "laptop-backup"
sys.path.insert(0, str(OPS))

import recovery  # noqa: E402

ZSTD = shutil.which("zstd")
requires_zstd = pytest.mark.skipif(ZSTD is None, reason="zstd binary unavailable")


def _real_zstd(data: bytes) -> bytes:
    completed = subprocess.run(
        [ZSTD, "-q", "-c"], input=data, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True
    )
    return completed.stdout


@requires_zstd
def test_real_zstd_accepts_downloaded_and_already_present_object(tmp_path: Path) -> None:
    archive = _real_zstd(b"real payload bytes" * 1000)
    digest = hashlib.sha256(archive).hexdigest()
    manifest = _manifest(digest, len(archive))
    manifest_sha = hashlib.sha256(manifest).hexdigest()
    binding = recovery.SourceBinding(digest=digest, size_bytes=len(archive), manifest_sha256=manifest_sha)
    transport = recovery.MappingTransport({digest: archive}, {manifest_sha: manifest})
    # No injected validator: the real zstd binary runs against the pinned fd path.
    first = recovery.download_to_quarantine(transport, binding, str(tmp_path))
    assert first["status"] == "downloaded"
    second = recovery.download_to_quarantine(transport, binding, str(tmp_path))
    assert second["status"] == "already_present"
    assert (tmp_path / f"{digest}.tar.zst").read_bytes() == archive
    assert list(tmp_path.glob("*.tmp")) == []


@requires_zstd
def test_real_zstd_rejects_malformed_bytes_and_cleans_up(tmp_path: Path) -> None:
    bad = b"definitely not a zstd stream" * 10
    digest = hashlib.sha256(bad).hexdigest()
    manifest = _manifest(digest, len(bad))
    manifest_sha = hashlib.sha256(manifest).hexdigest()
    binding = recovery.SourceBinding(digest=digest, size_bytes=len(bad), manifest_sha256=manifest_sha)
    transport = recovery.MappingTransport({digest: bad}, {manifest_sha: manifest})
    with pytest.raises(recovery.IntegrityError):
        recovery.download_to_quarantine(transport, binding, str(tmp_path))
    assert list(tmp_path.iterdir()) == []


def _manifest(digest: str, size: int, *, backup_id: str = "b1", schema: int = 1, source: str = "example-vault") -> bytes:
    return json.dumps(
        {
            "schema": schema,
            "source": source,
            "created_at": "2026-09-14T00:00:00+00:00",
            "archives": [
                {
                    "backup_id": backup_id,
                    "profile_id": "p1",
                    "created_at": "2026-09-14T00:00:00+00:00",
                    "sha256": digest,
                    "size_bytes": size,
                    "protected": False,
                }
            ],
        }
    ).encode()


def _fixture(payload: bytes):
    digest = hashlib.sha256(payload).hexdigest()
    manifest = _manifest(digest, len(payload))
    manifest_sha = hashlib.sha256(manifest).hexdigest()
    binding = recovery.SourceBinding(digest=digest, size_bytes=len(payload), manifest_sha256=manifest_sha)
    transport = recovery.MappingTransport({digest: payload}, {manifest_sha: manifest})
    return binding, transport


def _noop_zstd(path: str) -> None:
    assert os.path.exists(path)


def _raise_zstd(path: str) -> None:
    raise recovery.IntegrityError("zstd integrity check failed")


def test_download_publishes_verified_object_without_temp_leftover(tmp_path: Path) -> None:
    payload = b"payload-bytes" * 100
    binding, transport = _fixture(payload)
    result = recovery.download_to_quarantine(transport, binding, str(tmp_path), chunk_bytes=64, zstd_check=_noop_zstd)
    assert result["status"] == "downloaded"
    assert Path(result["path"]).read_bytes() == payload
    assert list(tmp_path.glob("*.tmp")) == []
    assert sorted(p.name for p in tmp_path.iterdir()) == [f"{binding.digest}.tar.zst"]


def test_matching_existing_object_is_idempotent_after_zstd() -> None:
    payload = b"abc" * 40
    binding, transport = _fixture(payload)
    tmp = Path(_tmpdir())
    try:
        recovery.download_to_quarantine(transport, binding, str(tmp), zstd_check=_noop_zstd)
        second = recovery.download_to_quarantine(transport, binding, str(tmp), zstd_check=_noop_zstd)
        assert second["status"] == "already_present"
    finally:
        _cleanup(tmp)


def test_existing_object_with_bad_zstd_is_not_accepted(tmp_path: Path) -> None:
    payload = b"abc" * 40
    binding, transport = _fixture(payload)
    recovery.download_to_quarantine(transport, binding, str(tmp_path), zstd_check=_noop_zstd)
    with pytest.raises(recovery.IntegrityError):
        recovery.download_to_quarantine(transport, binding, str(tmp_path), zstd_check=_raise_zstd)


def test_conflicting_existing_object_is_refused(tmp_path: Path) -> None:
    payload = b"expected" * 10
    binding, transport = _fixture(payload)
    target = tmp_path / f"{binding.digest}.tar.zst"
    target.write_bytes(b"different" * 10)
    with pytest.raises(recovery.ConflictError):
        recovery.download_to_quarantine(transport, binding, str(tmp_path), zstd_check=_noop_zstd)
    assert target.read_bytes() == b"different" * 10


def test_size_mismatch_fails_closed_and_cleans_temp(tmp_path: Path) -> None:
    payload = b"short"
    digest = hashlib.sha256(payload).hexdigest()
    manifest = _manifest(digest, 99)
    manifest_sha = hashlib.sha256(manifest).hexdigest()
    binding = recovery.SourceBinding(digest=digest, size_bytes=99, manifest_sha256=manifest_sha)
    transport = recovery.MappingTransport({digest: payload}, {manifest_sha: manifest})
    with pytest.raises(recovery.IntegrityError):
        recovery.download_to_quarantine(transport, binding, str(tmp_path), zstd_check=_noop_zstd)
    assert list(tmp_path.iterdir()) == []


def test_manifest_binding_is_enforced(tmp_path: Path) -> None:
    payload = b"abc"
    digest = hashlib.sha256(payload).hexdigest()
    manifest = _manifest("f" * 64, len(payload))
    manifest_sha = hashlib.sha256(manifest).hexdigest()
    binding = recovery.SourceBinding(digest=digest, size_bytes=len(payload), manifest_sha256=manifest_sha)
    transport = recovery.MappingTransport({digest: payload}, {manifest_sha: manifest})
    with pytest.raises(recovery.IntegrityError):
        recovery.download_to_quarantine(transport, binding, str(tmp_path), zstd_check=_noop_zstd)


def test_real_manifest_shape_is_parsed_strictly() -> None:
    digest = "a" * 64
    members = recovery.default_manifest_members(_manifest(digest, 5))
    assert members == [(digest, 5)]
    with pytest.raises(recovery.IntegrityError):
        recovery.default_manifest_members(_manifest(digest, 5, schema="1"))  # type: ignore[arg-type]
    with pytest.raises(recovery.IntegrityError):
        recovery.default_manifest_members(_manifest(digest, 5, schema=2))
    with pytest.raises(recovery.IntegrityError):
        recovery.default_manifest_members(_manifest(digest, 5, schema=True))  # type: ignore[arg-type]
    with pytest.raises(recovery.IntegrityError):
        recovery.default_manifest_members(b'{"schema":1,"source":"example-vault","archives":{}}')
    duplicate = json.dumps(
        {
            "schema": 1,
            "source": "example-vault",
            "archives": [
                {"backup_id": "b1", "sha256": digest, "size_bytes": 5},
                {"backup_id": "b1", "sha256": "b" * 64, "size_bytes": 6},
            ],
        }
    ).encode()
    with pytest.raises(recovery.IntegrityError):
        recovery.default_manifest_members(duplicate)


def test_invalid_binding_and_transport_are_refused(tmp_path: Path) -> None:
    for bad in ("../../etc/x", 123, None, "a" * 63):
        with pytest.raises(recovery.BindingError):
            recovery.SourceBinding(digest=bad, size_bytes=1, manifest_sha256="1" * 64)  # type: ignore[arg-type]
    with pytest.raises(recovery.BindingError):
        recovery.SourceBinding(digest="a" * 64, size_bytes=0, manifest_sha256="1" * 64)
    with pytest.raises(recovery.TransportError):
        recovery.CommandTransport([])
    with pytest.raises(recovery.TransportError):
        recovery.CommandTransport(["/bin/echo", "no-placeholder"])


def test_quarantine_symlink_is_refused(tmp_path: Path) -> None:
    payload = b"abc"
    binding, transport = _fixture(payload)
    real = tmp_path / "real"
    real.mkdir()
    link = tmp_path / "link"
    link.symlink_to(real)
    with pytest.raises(recovery.RecoveryError):
        recovery.download_to_quarantine(transport, binding, str(link), zstd_check=_noop_zstd)


def test_link_race_leaves_no_temp_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    payload = b"race" * 10
    binding, transport = _fixture(payload)

    def racing_link(src, dst, **kwargs):
        raise FileExistsError("race")

    monkeypatch.setattr(os, "link", racing_link)
    with pytest.raises(recovery.ConflictError):
        recovery.download_to_quarantine(transport, binding, str(tmp_path), zstd_check=_noop_zstd)
    monkeypatch.undo()
    assert list(tmp_path.glob("*.tmp")) == []
    assert [p.name for p in tmp_path.iterdir()] == []


def test_process_reader_streams_and_enforces_cap() -> None:
    script = "import sys;sys.stdout.buffer.write(b'z'*5000)"
    process = subprocess.Popen(
        [sys.executable, "-c", script], stdout=subprocess.PIPE, stderr=subprocess.DEVNULL
    )
    reader = recovery._ProcessReader(process, timeout=30, max_bytes=1 << 20)
    total = 0
    while True:
        chunk = reader.read(777)
        if not chunk:
            break
        total += len(chunk)
    reader.close()
    assert total == 5000

    capping = subprocess.Popen(
        [sys.executable, "-c", script], stdout=subprocess.PIPE, stderr=subprocess.DEVNULL
    )
    limited = recovery._ProcessReader(capping, timeout=30, max_bytes=100)
    with pytest.raises(recovery.IntegrityError):
        for _ in range(100):
            limited.read(64)
    limited.close()


def _spawn(script: str) -> subprocess.Popen:
    return subprocess.Popen(
        [sys.executable, "-c", script],
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )


def test_reader_deadline_interrupts_a_hung_child() -> None:
    process = _spawn("import time; time.sleep(30)")
    reader = recovery._ProcessReader(process, timeout=0.5, max_bytes=1 << 20)
    start = time.monotonic()
    with pytest.raises(recovery.TransportError):
        reader.read(1024)
    assert time.monotonic() - start < 5
    reader.close()
    assert process.poll() is not None  # child reaped, not leaked


def test_reader_deadline_bounds_a_large_read_and_slow_trickle() -> None:
    script = (
        "import sys,time\n"
        "for _ in range(60):\n"
        "    sys.stdout.buffer.write(b'x'); sys.stdout.buffer.flush(); time.sleep(1)\n"
    )
    process = _spawn(script)
    reader = recovery._ProcessReader(process, timeout=0.6, max_bytes=1 << 20)
    start = time.monotonic()
    # A large read must not block until 10 MB accumulate; it is deadline bounded.
    with pytest.raises(recovery.TransportError):
        reader.read(10_000_000)
    assert time.monotonic() - start < 5
    reader.close()
    assert process.poll() is not None


def test_reader_deadline_covers_descendant_holding_stdout() -> None:
    # The direct child exits immediately, but its grandchild inherits stdout and
    # sleeps, so the pipe never reaches EOF.
    script = (
        "import subprocess,sys\n"
        "subprocess.Popen([sys.executable,'-c','import time;time.sleep(30)'])\n"
    )
    process = _spawn(script)
    reader = recovery._ProcessReader(process, timeout=0.6, max_bytes=1 << 20)
    start = time.monotonic()
    with pytest.raises(recovery.TransportError):
        reader.read(1024)
    assert time.monotonic() - start < 5
    reader.close()


def test_command_transport_uses_distinct_object_and_manifest_selectors() -> None:
    object_argv = [sys.executable, "-c", "import sys;sys.stdout.buffer.write(b'OBJECT')", "{digest}"]
    manifest_argv = [sys.executable, "-c", "import sys;sys.stdout.buffer.write(b'MANIFEST')", "{digest}"]
    transport = recovery.CommandTransport(object_argv, manifest_argv)
    assert transport.open("a" * 64).read() == b"OBJECT"
    assert transport.open_manifest("b" * 64).read() == b"MANIFEST"


def _tmpdir() -> str:
    import tempfile

    return tempfile.mkdtemp()


def _cleanup(path: Path) -> None:
    for child in path.iterdir():
        child.unlink()
    path.rmdir()
