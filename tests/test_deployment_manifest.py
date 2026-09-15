from __future__ import annotations

from dataclasses import FrozenInstanceError, replace
from pathlib import Path

import pytest

from game_control.deployment_manifest import (
    DeploymentManifest,
    DirectorySpec,
    FileSpec,
    NamespaceSpec,
    SymlinkSpec,
    get_manifest,
    manifest_digest,
)


def test_manifest_is_typed_frozen_and_exactly_sized() -> None:
    manifest = get_manifest()
    assert isinstance(manifest, DeploymentManifest)
    assert len(manifest.files) == 48
    assert len(manifest.directories) == 50
    # Exact projection size, not a lower bound: every ``src/game_control``
    # module must be projected. baseline 832a619 shipped 82 sources while this
    # assertion still read 80; the integration adds ``startup_estimates.py``
    # (baseline omission) and the KubeJS compatibility module, so the reviewed
    # count is 84 sources and 136 runtime files.
    assert len(manifest.runtime_sources) == 84
    assert len(manifest.runtime_files_for()) == 136
    assert manifest.generated_entry_point.name == "horizon"
    assert manifest.generated_entry_point.target == "/opt/game-control/.venv/bin/horizon"
    assert manifest.generated_entry_point.module == "game_control.cli:main"
    assert all(isinstance(value, tuple) for value in (manifest.files, manifest.directories, manifest.runtime_sources))
    with pytest.raises(FrozenInstanceError):
        manifest.schema_version = 2  # type: ignore[misc]


def test_manifest_projections_remap_only_targets() -> None:
    manifest = get_manifest()
    projected = manifest.files_for(Path("/stage"))
    assert projected[0].target.startswith("/stage/")
    assert projected[0].source == manifest.files[0].source
    assert manifest.links_for(Path("/stage"))[0].target.startswith("/stage/")
    assert manifest.links_for(Path("/stage"))[0].link_target.startswith("/")


def test_manifest_validation_checks_real_sources_without_import_side_effects() -> None:
    get_manifest().validate(Path("."))
    assert len(manifest_digest()) == 64


def test_wave5_libexec_projection_is_fixed_and_alias_free() -> None:
    from game_control.deployment_manifest import ABSENT_LIBEXEC_NAMES, FIXED_LIBEXEC_NAMES

    manifest = get_manifest()
    names = {
        spec.target.rsplit("/", 1)[-1]
        for spec in manifest.files
        if spec.target.startswith("/usr/local/libexec/")
    }
    assert names == set(FIXED_LIBEXEC_NAMES)
    assert names.isdisjoint(ABSENT_LIBEXEC_NAMES)


def test_jvm_directory_is_traversable_but_not_writable() -> None:
    directory = next(
        spec for spec in get_manifest().directories
        if spec.target == "/etc/game-control/jvm"
    )
    assert directory.mode == 0o755
    assert directory.owner == directory.group == "root"


def test_generated_console_script_is_metadata_only_for_alternate_roots() -> None:
    manifest = get_manifest()
    assert all(
        spec.target != manifest.generated_entry_point.target
        for spec in (*manifest.files, *manifest.runtime_support)
    )
    assert all(
        spec.target != manifest.generated_entry_point.target
        for spec in manifest.runtime_files_for()
    )


@pytest.mark.parametrize("source", ["", "/absolute.py", "a/../b.py", "a\\b.py", "."])
def test_file_spec_rejects_unsafe_source(source: str) -> None:
    with pytest.raises(ValueError):
        FileSpec(source, "/opt/example", 0o644)


def test_schema_rejects_bool_modes_and_mutable_namespace_fields() -> None:
    with pytest.raises(ValueError):
        FileSpec("a", "/opt/example", True)  # type: ignore[arg-type]
    with pytest.raises(ValueError):
        NamespaceSpec("example", "/opt/example", ["one"])  # type: ignore[arg-type]
    with pytest.raises(ValueError):
        NamespaceSpec("example", "/opt/example", (), ["prefix-"])  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("files", "directories", "symlinks"),
    (
        ((FileSpec("a", "/x", 0o644),), (DirectorySpec("/x/y", 0o755),), ()),
        ((FileSpec("a", "/x/y", 0o644),), (DirectorySpec("/x", 0o755),), ()),
        ((FileSpec("a", "/x", 0o644),), (), (SymlinkSpec("/x/y", "/target"),)),
        ((FileSpec("a", "/x/y", 0o644),), (), (SymlinkSpec("/x", "/target"),)),
    ),
)
def test_schema_rejects_parent_collisions(files, directories, symlinks) -> None:
    manifest = get_manifest()
    with pytest.raises(ValueError, match="parent collision"):
        DeploymentManifest(
            manifest.schema_version,
            manifest.profiles,
            manifest.files + files,
            manifest.directories + directories,
            manifest.symlinks + symlinks,
            manifest.namespaces,
            manifest.retired,
            manifest.secrets,
            manifest.databases,
            manifest.runtime_manifest,
            manifest.generated_entry_point,
            manifest.relay_modes,
            manifest.runtime_sources,
            manifest.runtime_support,
        )


def test_schema_rejects_mutable_manifest_collections_and_empty_profile() -> None:
    manifest = get_manifest()
    with pytest.raises(ValueError, match="immutable tuple"):
        replace(manifest, files=list(manifest.files))  # type: ignore[arg-type]
    with pytest.raises(ValueError):
        replace(manifest.profiles[0], id="", profile_source="config/profiles/.toml", runner_source="config/runner/.json", unit=".service")
