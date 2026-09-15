from __future__ import annotations

import hashlib
import json
import shutil
import subprocess
import zipfile
from argparse import Namespace
from pathlib import Path

import pytest

from game_control import sunlit_kubejs_compat as MODULE
from game_control import sunlit_manifest as MANIFEST_MODULE
from game_control import sunlit_stage as STAGE_MODULE
from game_control import sunlit_update as UPDATE_MODULE

KINDS = ("fire", "water")
STATEMENT = '    JsonIO.write("loot/badge_reward/${type.type}_type_gym.json", lootTable);'
REPLACEMENT = '    // JsonIO.write("loot/badge_reward/${type.type}_type_gym.json", lootTable);'
GENERATOR_BODY = (
    "// synthetic staging fixture\r\n"
    "function generate(type) {\r\n"
    f"{STATEMENT}\r\n"
    "}\r\n"
)
REFERENCE = GENERATOR_BODY.encode()
GUARDED = REFERENCE.replace(STATEMENT.encode(), REPLACEMENT.encode(), 1)


def _digest(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _fixture(
    tmp_path: Path,
    *,
    kinds=KINDS,
    mutate: str | None = None,
    drop: str | None = None,
    extra: str | None = None,
    malformed: str | None = None,
    generator: bytes | None = None,
    versions=("1.2.3",),
    sibling: bool = False,
):
    root = tmp_path / "runtime"
    generator_path = root.joinpath(*MODULE.GENERATOR_PATH.split("/"))
    generator_path.parent.mkdir(parents=True)
    payload = REFERENCE if generator is None else generator
    generator_path.write_bytes(payload)
    generator_path.chmod(0o644)
    directory = root.joinpath(*MODULE.REWARD_ROOT.split("/"))
    directory.mkdir(parents=True, exist_ok=True)
    for kind in kinds:
        if kind == drop:
            continue
        table = MODULE.expected_reward_table(kind)
        if kind == mutate:
            table["pools"][0]["rolls"] = 2
        name = f"{kind}{MODULE.REWARD_SUFFIX}"
        text = "{ not json" if kind == malformed else json.dumps(table, indent=4) + "\n"
        (directory / name).write_text(text, encoding="utf-8")
    if extra:
        (directory / extra).write_text("{}\n", encoding="utf-8")
    if sibling:
        (directory / "notes.txt").write_text("unrelated\n", encoding="utf-8")
        (root / "kubejs/README.txt").write_text("unrelated\n", encoding="utf-8")
    guard = MODULE.GymLootGuard(
        path=MODULE.GENERATOR_PATH,
        reward_root=MODULE.REWARD_ROOT,
        reward_kinds=tuple(kinds),
        versions=tuple(versions),
        enabled_sha256=_digest(REFERENCE),
        guarded_sha256=_digest(GUARDED),
        known_disabled_sha256=(_digest(GUARDED),),
        statement=STATEMENT,
        replacement=REPLACEMENT,
    )
    return root, guard, generator_path, payload


def _tree_state(root: Path) -> dict[str, str]:
    return {
        path.relative_to(root).as_posix(): _digest(path.read_bytes())
        for path in sorted(root.rglob("*"))
        if path.is_file()
    }


def _inject_reviewed(monkeypatch, guard) -> None:
    """Test-only seam: fixtures supply the code-reviewed record, not the manifest."""
    monkeypatch.setattr(MODULE, "reviewed_guard_registry", lambda: {guard.transform_id: guard})


@pytest.mark.parametrize(
    "field",
    ["path", "reward_root", "reward_kinds", "versions", "enabled_sha256", "guarded_sha256",
     "known_disabled_sha256", "statement", "replacement"],
)
def test_manifest_cannot_alter_any_reviewed_field(tmp_path: Path, field: str):
    root, guard, generator_path, _ = _fixture(tmp_path)
    before = _tree_state(root)
    altered = guard.as_policy()
    malicious = b"// pwned\n"
    if field == "path":
        altered["path"] = "config/other.js"
    elif field == "reward_root":
        altered["reward_root"] = "kubejs/data/other"
    elif field == "reward_kinds":
        altered["reward_kinds"] = ["stellar"]
    elif field == "versions":
        altered["versions"] = ["9.9.9"]
    elif field == "enabled_sha256":
        altered["enabled_sha256"] = _digest(malicious)
    elif field == "guarded_sha256":
        altered["guarded_sha256"] = _digest(malicious)
        altered["known_disabled_sha256"] = [_digest(malicious)]
    elif field == "known_disabled_sha256":
        altered["known_disabled_sha256"] = [_digest(malicious), altered["guarded_sha256"]]
    elif field == "statement":
        altered["statement"] = REFERENCE.decode()
    elif field == "replacement":
        altered["replacement"] = malicious.decode()
    policy = {"compatibility_transforms": [altered]}

    with pytest.raises(MODULE.CompatTransformError, match="altered compatibility transform") as excinfo:
        MODULE.parse_policy_transforms(policy)
    assert field in str(excinfo.value)
    with pytest.raises(MODULE.CompatTransformError):
        MODULE.apply_policy_transforms(root, policy, "1.2.3")
    assert _tree_state(root) == before


def test_reproducer_arbitrary_replacement_is_refused(tmp_path: Path):
    """Regression for /tmp/ku-review/repro_policy_authority.py."""
    root, guard, generator_path, _ = _fixture(tmp_path)
    original = generator_path.read_bytes()
    malicious = b"// pwned: arbitrary replacement content\n"
    policy = {
        "transform_id": MODULE.TRANSFORM_ID,
        "path": MODULE.GENERATOR_PATH,
        "reward_root": MODULE.REWARD_ROOT,
        "reward_kinds": list(KINDS),
        "versions": ["1.2.3"],
        "enabled_sha256": _digest(original),
        "guarded_sha256": _digest(malicious),
        "known_disabled_sha256": [_digest(malicious)],
        "statement": original.decode(),
        "replacement": malicious.decode(),
    }
    with pytest.raises(MODULE.CompatTransformError, match="altered compatibility transform"):
        MODULE.apply_policy_transforms(root, {"compatibility_transforms": [policy]}, "1.2.3")
    assert generator_path.read_bytes() == original
    redirected = dict(policy, path="config/other.js", statement="x", replacement="y")
    with pytest.raises(MODULE.CompatTransformError, match="altered compatibility transform"):
        MODULE.parse_policy_transforms({"compatibility_transforms": [redirected]})


def test_reviewed_record_is_the_single_authority(tmp_path: Path):
    guard = MODULE.reviewed_gym_loot_guard()
    assert MODULE.reviewed_guard_registry() == {MODULE.TRANSFORM_ID: guard}
    assert len(set(guard.reward_kinds)) == 18
    # The reviewed record round-trips through its own policy form, and the
    # parsed result is the reviewed object rather than the declared copy.
    declared = guard.as_policy()
    parsed = MODULE.parse_policy_transforms({"compatibility_transforms": [declared]})
    assert parsed == (guard,)
    assert parsed[0] is guard


def test_policy_requires_every_reviewed_field(tmp_path: Path):
    guard = MODULE.reviewed_gym_loot_guard()
    for field in guard.as_policy():
        broken = guard.as_policy()
        broken.pop(field)
        with pytest.raises(MODULE.CompatTransformError):
            MODULE.parse_policy_transforms({"compatibility_transforms": [broken]})


def test_applies_reviewed_transform_and_reports_provenance(tmp_path: Path, monkeypatch):
    root, guard, generator_path, _ = _fixture(tmp_path, sibling=True)
    _inject_reviewed(monkeypatch, guard)
    before = _tree_state(root)

    record = MODULE.apply_policy_transforms(root, {"compatibility_transforms": [guard.as_policy()]}, "1.2.3")

    assert len(record) == 1
    entry = record[0]
    assert entry["transform_id"] == MODULE.TRANSFORM_ID
    assert entry["action"] == "disabled_write"
    assert entry["version"] == "1.2.3"
    assert entry["path"] == MODULE.GENERATOR_PATH
    assert entry["generator_sha256_before"] == before[MODULE.GENERATOR_PATH]
    assert entry["generator_sha256_after"] == _digest(GUARDED)
    assert entry["reward_tables_checked"] == len(KINDS)
    assert generator_path.read_bytes() == GUARDED
    assert generator_path.stat().st_mode & 0o777 == 0o644


def test_all_baked_tables_must_match_the_generator_output(tmp_path: Path):
    root, guard, _, _ = _fixture(tmp_path)
    assert MODULE.verify_reward_tables(root, guard)["count"] == len(KINDS)


@pytest.mark.parametrize(
    ("kwargs", "expected"),
    [
        ({"mutate": "water"}, "does not match the reviewed generator output"),
        ({"drop": "fire"}, "missing"),
        ({"extra": f"stellar{MODULE.REWARD_SUFFIX}"}, "unexpected"),
        ({"malformed": "fire"}, "not valid JSON"),
    ],
)
def test_changed_reward_sets_fail_closed(tmp_path: Path, kwargs, expected):
    root, guard, generator_path, _ = _fixture(tmp_path, **kwargs)
    before = generator_path.read_bytes()

    with pytest.raises(MODULE.CompatTransformError, match=expected):
        MODULE.apply_guard(root, guard, "1.2.3")

    assert generator_path.read_bytes() == before
    assert {path.name for path in generator_path.parent.iterdir()} == {generator_path.name}


def test_missing_reward_directory_fails_closed(tmp_path: Path):
    root, guard, generator_path, _ = _fixture(tmp_path)
    shutil.rmtree(root.joinpath(*MODULE.REWARD_ROOT.split("/")))
    with pytest.raises(MODULE.CompatTransformError, match="missing or unsafe"):
        MODULE.apply_guard(root, guard, "1.2.3")
    assert generator_path.exists()


def test_unrelated_reward_files_are_ignored(tmp_path: Path):
    root, guard, _, _ = _fixture(tmp_path, sibling=True)
    record = MODULE.apply_guard(root, guard, "1.2.3")
    assert record["action"] == "disabled_write"
    assert (root / "kubejs/README.txt").read_text(encoding="utf-8") == "unrelated\n"
    directory = root.joinpath(*MODULE.REWARD_ROOT.split("/"))
    assert (directory / "notes.txt").read_text(encoding="utf-8") == "unrelated\n"
    assert (directory / "fire_type_gym.json").exists()


def test_changed_generator_fails_closed_with_actionable_error(tmp_path: Path):
    changed = REFERENCE.replace(b"function generate", b"function generate2")
    root, guard, generator_path, _ = _fixture(tmp_path, generator=changed)

    with pytest.raises(MODULE.CompatTransformError) as excinfo:
        MODULE.apply_guard(root, guard, "1.2.3")

    message = str(excinfo.value)
    assert "unrecognised KubeJS gym-loot generator" in message
    assert MODULE.TRANSFORM_ID in message
    assert "review" in message
    assert generator_path.read_bytes() == changed


def test_unreviewed_version_fails_closed(tmp_path: Path):
    root, guard, generator_path, _ = _fixture(tmp_path, versions=("9.9.9",))
    before = generator_path.read_bytes()

    with pytest.raises(MODULE.CompatTransformError, match="bound to artifact version"):
        MODULE.apply_guard(root, guard, "1.2.3")

    assert generator_path.read_bytes() == before


def test_guarded_generator_is_idempotent(tmp_path: Path):
    root, guard, generator_path, _ = _fixture(tmp_path, generator=GUARDED)
    first = MODULE.apply_guard(root, guard, "1.2.3")
    second = MODULE.apply_guard(root, guard, "1.2.3")
    assert first["action"] == "already_disabled"
    assert second["action"] == "already_disabled"
    assert second["generator_sha256_before"] == second["generator_sha256_after"] == _digest(GUARDED)
    assert generator_path.read_bytes() == GUARDED
    assert {path.name for path in generator_path.parent.iterdir()} == {generator_path.name}


def test_older_already_disabled_generator_is_accepted(tmp_path: Path):
    older = b"// everything is disabled already\r\n// " + STATEMENT.encode() + b"\r\n"
    root, guard, generator_path, _ = _fixture(tmp_path, generator=older)
    guard = MODULE.GymLootGuard(
        path=guard.path,
        reward_root=guard.reward_root,
        reward_kinds=guard.reward_kinds,
        versions=guard.versions,
        enabled_sha256=guard.enabled_sha256,
        guarded_sha256=guard.guarded_sha256,
        known_disabled_sha256=(*guard.known_disabled_sha256, _digest(older)),
        statement=guard.statement,
        replacement=guard.replacement,
    )
    record = MODULE.apply_guard(root, guard, "1.2.3")
    assert record["action"] == "already_disabled"
    assert generator_path.read_bytes() == older


def test_no_partial_modification_when_preflight_fails(tmp_path: Path):
    root, guard, generator_path, _ = _fixture(tmp_path, mutate="fire", sibling=True)
    before = _tree_state(root)

    with pytest.raises(MODULE.CompatTransformError):
        MODULE.apply_policy_transforms(root, {"compatibility_transforms": [guard.as_policy()]}, "1.2.3")

    assert _tree_state(root) == before


def test_transformed_fixture_passes_node_syntax_check(tmp_path: Path):
    if shutil.which("node") is None:  # pragma: no cover - node is absent locally
        pytest.skip("node is unavailable")
    root, guard, generator_path, _ = _fixture(tmp_path)
    MODULE.apply_guard(root, guard, "1.2.3")
    result = subprocess.run(["node", "--check", str(generator_path)], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr


def test_policy_parsing_rejects_unknown_or_duplicate_transforms(tmp_path: Path, monkeypatch):
    _, guard, _, _ = _fixture(tmp_path)
    _inject_reviewed(monkeypatch, guard)
    assert MODULE.parse_policy_transforms({}) == ()
    assert len(MODULE.parse_policy_transforms({"compatibility_transforms": [guard.as_policy()]})) == 1
    unknown = dict(guard.as_policy(), transform_id="something-else")
    with pytest.raises(MODULE.CompatTransformError, match="unsupported compatibility transform"):
        MODULE.parse_policy_transforms({"compatibility_transforms": [unknown]})
    with pytest.raises(MODULE.CompatTransformError, match="declares a transform twice"):
        MODULE.parse_policy_transforms(
            {"compatibility_transforms": [guard.as_policy(), guard.as_policy()]}
        )
    malformed = guard.as_policy()
    malformed.pop("statement")
    with pytest.raises(MODULE.CompatTransformError):
        MODULE.parse_policy_transforms({"compatibility_transforms": [malformed]})
    with pytest.raises(MODULE.CompatTransformError):
        MODULE.parse_policy_transforms({"compatibility_transforms": "nope"})


def test_reviewed_spec_matches_primary_reviewed_bytes():
    guard = MODULE.reviewed_gym_loot_guard()
    assert guard.enabled_sha256 == "25cb09d72986672bcb88ec25832c8c27a0a361597d8aab3f2bb0a225c1a78b72"
    assert guard.guarded_sha256 == "5a95e607e513667157f761c358bf97452c2e0cad77a4dc6d795b8b09205be930"
    assert "91269a070b2645aac130fc91d206603501ea9a8a85e0a6242b1da6fd21725a15" in guard.known_disabled_sha256
    assert guard.versions == ("1.1.4-SSV4.1.5",)
    assert guard.path == "kubejs/server_scripts/cobblemon/datagen/generateLootTables.js"
    assert len(MODULE.REWARD_KINDS) == 18
    assert sorted(MODULE.reviewed_compatibility_transforms()[0]) == sorted(guard.as_policy())
    assert MODULE.reviewed_compatibility_transforms()[0]["versions"] == ["1.1.4-SSV4.1.5"]
    assert len(set(MODULE.REWARD_KINDS)) == len(MODULE.REWARD_KINDS) == 18


def test_paths_are_validated_lexically(tmp_path: Path):
    _, guard, _, _ = _fixture(tmp_path)
    for bad in ("kubejs//gen.js", "./kubejs/gen.js", "kubejs/../gen.js", "/kubejs/gen.js", "kubejs/", " "):
        broken = guard.as_policy()
        broken["path"] = bad
        with pytest.raises(MODULE.CompatTransformError, match="safe relative path"):
            MODULE.parse_policy_transforms({"compatibility_transforms": [broken]})


def test_manifest_without_transforms_stages_without_a_guard(tmp_path: Path):
    archive, prior, manifest, _guard = _staging_fixture(tmp_path)
    document = json.loads(manifest.read_text(encoding="utf-8"))
    document["runtime_policy"].pop("compatibility_transforms")
    body = dict(document)
    body.pop("manifest_sha256", None)
    document["manifest_sha256"] = _digest(
        json.dumps(body, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()
    )
    manifest.write_text(json.dumps(document), encoding="utf-8")
    candidate = tmp_path / "candidate"
    report = STAGE_MODULE.stage(Namespace(
        manifest=manifest, archive=archive, prior_runtime=prior, candidate_root=candidate,
    ))
    # A legacy manifest still stages; the guard is simply not declared, and the
    # provenance record states so explicitly rather than implying verification.
    assert report["compatibility_transforms"] == []
    staged = candidate.joinpath("runtime", *MODULE.GENERATOR_PATH.split("/"))
    assert staged.read_bytes() == REFERENCE


def test_manifest_declares_the_reviewed_transform(tmp_path: Path):
    archive = tmp_path / "pack.zip"
    with zipfile.ZipFile(archive, "w") as output:
        output.writestr("config/a.txt", b"sunlit-data")
    overlay = tmp_path / "overlay"
    overlay.write_bytes(b"overlay")
    document = MANIFEST_MODULE.make_manifest(Namespace(
        archive=archive, version="1.0.0", project_id="123", file_id="456",
        url="https://example.invalid/artifact.zip", archive_size=archive.stat().st_size,
        archive_sha256=_digest(archive.read_bytes()), overlay_source=overlay,
        overlay_destination="overlay.txt", overlay_sha256=_digest(b"overlay"),
    ))
    transforms = document["runtime_policy"]["compatibility_transforms"]
    assert [item["transform_id"] for item in transforms] == [MODULE.TRANSFORM_ID]
    # The transform record is inside the manifest self-digest, so staging and
    # promotion validate the same reviewed bytes.
    assert STAGE_MODULE._canonical_manifest(document) == document["manifest_sha256"]


def _staging_fixture(tmp_path: Path, *, mutate: str | None = None):
    archive = tmp_path / "pack.zip"
    config = b"setting = 10\n"
    table = json.dumps(MODULE.expected_reward_table("fire"), indent=4) + "\n"
    if mutate == "table":
        table = json.dumps({"pools": []}) + "\n"
    generator = REFERENCE
    if mutate == "generator":
        generator = generator.replace(b"function generate", b"function generate2")
    with zipfile.ZipFile(archive, "w") as output:
        output.writestr("config/base.toml", config)
        output.writestr("kubejs/README.txt", b"unrelated\n")
        output.writestr(MODULE.GENERATOR_PATH, generator)
        output.writestr(f"{MODULE.REWARD_ROOT}/fire{MODULE.REWARD_SUFFIX}", table)
    overlay = tmp_path / "metrics.jar"
    overlay.write_bytes(b"metrics")
    libraries = tmp_path / "libraries"
    libraries.mkdir()
    entries = []
    with zipfile.ZipFile(archive) as source:
        for info in source.infolist():
            payload = source.read(info)
            entries.append({"path": info.filename, "size": len(payload), "sha256": _digest(payload)})
    prior = tmp_path / "prior"
    (prior / "world").mkdir(parents=True)
    (prior / "world/data").write_bytes(b"world")
    (prior / "ops.json").write_bytes(b"ops")
    updated = b"setting = 15\n"
    guard = MODULE.GymLootGuard(
        path=MODULE.GENERATOR_PATH,
        reward_root=MODULE.REWARD_ROOT,
        reward_kinds=("fire",),
        versions=("v1",),
        enabled_sha256=_digest(REFERENCE),
        guarded_sha256=_digest(GUARDED),
        known_disabled_sha256=(_digest(GUARDED),),
        statement=STATEMENT,
        replacement=REPLACEMENT,
    )
    document = {
        "manifest_version": 1,
        "profile_id": "minecraft-sunlit-cobblemon",
        "artifact": {
            "version": "v1",
            "archive": {"size": archive.stat().st_size, "sha256": _digest(archive.read_bytes())},
        },
        "archive": {"roots": sorted({item["path"].split("/", 1)[0] for item in entries}), "entries": entries},
        "overlay": {"source": str(overlay), "destination": "mods/metrics.jar", "sha256": _digest(b"metrics")},
        "runtime_policy": {
            "persistent_dirs": ["world"], "persistent_files": ["ops.json"],
            "required_paths": ["world", "ops.json"], "mutable_vendor_dirs": ["config"],
            "empty_mutable_dirs": ["logs"], "fixed_symlinks": {"libraries": str(libraries)},
            "text_overrides": [{
                "path": "config/base.toml", "before_sha256": _digest(config),
                "after_sha256": _digest(updated), "old": "setting = 10", "new": "setting = 15",
            }],
            "compatibility_transforms": [guard.as_policy()],
        },
    }
    canonical = json.dumps(document, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()
    document["manifest_sha256"] = _digest(canonical)
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps(document), encoding="utf-8")
    return archive, prior, manifest, guard


def test_stage_records_transform_provenance(tmp_path: Path, monkeypatch):
    archive, prior, manifest, guard = _staging_fixture(tmp_path)
    _inject_reviewed(monkeypatch, guard)
    candidate = tmp_path / "candidate"
    report = STAGE_MODULE.stage(Namespace(
        manifest=manifest, archive=archive, prior_runtime=prior, candidate_root=candidate,
    ))
    record = report["compatibility_transforms"][0]
    assert record["action"] == "disabled_write"
    assert record["version"] == "v1"
    assert record["reward_tables_checked"] == 1
    staged = candidate.joinpath("runtime", *MODULE.GENERATOR_PATH.split("/"))
    assert staged.read_bytes() == GUARDED
    assert candidate.joinpath("runtime/kubejs/README.txt").read_text(encoding="utf-8") == "unrelated\n"
    published = json.loads((candidate / "candidate.json").read_text(encoding="utf-8"))
    assert published["compatibility_transforms"] == report["compatibility_transforms"]
    assert published["manifest_sha256"] == report["manifest_sha256"]
    # The promotion contract reads exactly these candidate fields and the
    # manifest self-digest that carries the reviewed transform.
    assert published["active"] is False and published["version"] == "v1"
    document = json.loads(manifest.read_text(encoding="utf-8"))
    assert STAGE_MODULE._canonical_manifest(document) == published["manifest_sha256"]
    assert document["runtime_policy"]["compatibility_transforms"][0]["transform_id"] == MODULE.TRANSFORM_ID


@pytest.mark.parametrize("mutate", ["generator", "table"])
def test_stage_fails_closed_and_removes_the_candidate(tmp_path: Path, mutate: str, monkeypatch):
    archive, prior, manifest, guard = _staging_fixture(tmp_path, mutate=mutate)
    _inject_reviewed(monkeypatch, guard)
    candidate = tmp_path / "candidate"
    with pytest.raises(MODULE.CompatTransformError):
        STAGE_MODULE.stage(Namespace(
            manifest=manifest, archive=archive, prior_runtime=prior, candidate_root=candidate,
        ))
    assert not candidate.exists()


def test_stage_rejects_unknown_declared_transform(tmp_path: Path):
    archive, prior, manifest, _guard = _staging_fixture(tmp_path)
    document = json.loads(manifest.read_text(encoding="utf-8"))
    document["runtime_policy"]["compatibility_transforms"][0]["transform_id"] = "unreviewed"
    body = dict(document)
    body.pop("manifest_sha256", None)
    document["manifest_sha256"] = _digest(
        json.dumps(body, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()
    )
    manifest.write_text(json.dumps(document), encoding="utf-8")
    candidate = tmp_path / "candidate"
    with pytest.raises(MODULE.CompatTransformError, match="unsupported compatibility transform"):
        STAGE_MODULE.stage(Namespace(
            manifest=manifest, archive=archive, prior_runtime=prior, candidate_root=candidate,
        ))
    assert not candidate.exists()


def test_updater_stage_propagates_the_transform_refusal(tmp_path: Path, monkeypatch):
    version = "1.1.4-SSV4.1.5"
    staging = tmp_path / "staging"
    state = tmp_path / "state"
    staging.mkdir()
    state.mkdir()
    (state / "world").mkdir()
    (state / "world/data").write_bytes(b"world")
    (state / "ops.json").write_bytes(b"ops")
    for name in ("server.properties", "user_jvm_args.txt", "eula.txt", "whitelist.json",
                 "banned-ips.json", "banned-players.json", "server-icon.png", "usercache.json",
                 "usernamecache.json"):
        (state / name).write_bytes(b"state")
    overlay = tmp_path / "Prometheus.jar"
    overlay.write_bytes(b"metrics")
    monkeypatch.setattr(UPDATE_MODULE, "STAGING_ROOT", staging)
    monkeypatch.setattr(UPDATE_MODULE, "STATE_ROOT", state)
    monkeypatch.setattr(UPDATE_MODULE, "_require_space", lambda *_, **__: None)
    monkeypatch.setattr(
        UPDATE_MODULE, "_trusted_release_overlay",
        lambda: UPDATE_MODULE.TrustedOverlay(overlay, overlay.stat().st_size, _digest(b"metrics")),
    )
    archive = tmp_path / "server-pack.zip"
    changed = REFERENCE.replace(b"function generate", b"function generate2")
    with zipfile.ZipFile(archive, "w") as bundle:
        bundle.writestr("config/base.toml", b"setting = 10\n")
        bundle.writestr(MODULE.GENERATOR_PATH, changed)
        bundle.writestr(f"{MODULE.REWARD_ROOT}/fire{MODULE.REWARD_SUFFIX}",
                        json.dumps(MODULE.expected_reward_table("fire")) + "\n")
    entries = [
        {"path": info.filename, "size": info.file_size,
         "sha256": _digest(zipfile.ZipFile(archive).read(info))}
        for info in zipfile.ZipFile(archive).infolist()
    ]
    guard = MODULE.GymLootGuard(
        path=MODULE.GENERATOR_PATH, reward_root=MODULE.REWARD_ROOT, reward_kinds=("fire",),
        versions=(version,), enabled_sha256=_digest(REFERENCE), guarded_sha256=_digest(GUARDED),
        known_disabled_sha256=(_digest(GUARDED),), statement=STATEMENT, replacement=REPLACEMENT,
    )
    _inject_reviewed(monkeypatch, guard)
    document = {
        "manifest_version": 1,
        "profile_id": "minecraft-sunlit-cobblemon",
        "artifact": {
            "project_id": UPDATE_MODULE.PROJECT_ID,
            "file_id": "42",
            "version": version,
            "url": "https://example.invalid/archive.zip",
            "archive": {"size": archive.stat().st_size, "sha256": _digest(archive.read_bytes())},
        },
        "archive": {"roots": sorted({item["path"].split("/", 1)[0] for item in entries}), "entries": entries},
        "overlay": {"source": str(overlay), "destination": UPDATE_MODULE.OVERLAY_DESTINATION,
                    "size": overlay.stat().st_size, "sha256": _digest(b"metrics")},
        "runtime_policy": {
            "persistent_dirs": ["world"], "persistent_files": ["ops.json"],
            "required_paths": ["world", "ops.json"], "mutable_vendor_dirs": ["config"],
            "empty_mutable_dirs": ["logs"], "fixed_symlinks": {},
            "text_overrides": [], "compatibility_transforms": [guard.as_policy()],
        },
    }
    body = dict(document)
    document["manifest_sha256"] = _digest(
        json.dumps(body, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()
    )
    root = staging / f"sunlit-{version}"
    root.mkdir(parents=True)
    (root / "server-pack.zip").write_bytes(archive.read_bytes())
    (root / "manifest.json").write_text(json.dumps(document), encoding="utf-8")
    identity = root / "update-operation-id"
    identity.write_text("11111111-2222-3333-4444-555555555555\n", encoding="ascii")
    identity.chmod(0o600)
    release = {"version": version, "file_id": "42", "size": archive.stat().st_size,
               "url": "https://example.invalid/archive.zip"}

    with pytest.raises(UPDATE_MODULE.UpdateError, match="candidate staging failed") as excinfo:
        UPDATE_MODULE._stage(release)

    cause = excinfo.value.__cause__
    assert isinstance(cause, MODULE.CompatTransformError)
    assert "unrecognised KubeJS gym-loot generator" in str(cause)
    assert not (root / "candidate").exists()
