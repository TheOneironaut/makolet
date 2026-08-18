from __future__ import annotations

import json
import shutil
from pathlib import Path

import pytest

from scripts import check_licenses

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
POLICY_FILES = (
    "docs/research/dependency-license-policy.toml",
    "uv.lock",
    "build-constraints.txt",
    "sbom.cdx.json",
    "sbom.build.cdx.json",
    "sbom.runtime-linux.cdx.json",
    "THIRD_PARTY_NOTICES.md",
    "docs/research/dependency-license-audit.md",
)


def _copy_policy_fixture(tmp_path: Path) -> Path:
    for relative_name in POLICY_FILES:
        source = REPOSITORY_ROOT / relative_name
        destination = tmp_path / relative_name
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, destination)
    return tmp_path


def _replace_once(path: Path, old: str, new: str) -> None:
    text = path.read_text(encoding="utf-8")
    assert text.count(old) == 1
    path.write_text(text.replace(old, new), encoding="utf-8")


def test_repository_license_policy_covers_every_frozen_inventory() -> None:
    result = check_licenses.check_repository(REPOSITORY_ROOT)

    assert result.locked_count == 84
    assert result.build_count == 6
    assert result.reviewed_count == 87
    assert result.debian_count == 97


def test_ci_uses_the_frozen_inventory_gate_without_partial_tool_output() -> None:
    workflow = (REPOSITORY_ROOT / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8")

    assert "uv run python scripts/check_licenses.py\n" in workflow
    assert ".ci-licenses.json" not in workflow
    assert "uv run pip-licenses" not in workflow


def test_notices_use_the_canonical_frozen_inventory_gate() -> None:
    notices = (REPOSITORY_ROOT / "THIRD_PARTY_NOTICES.md").read_text(encoding="utf-8")

    assert "uv run python scripts/check_licenses.py\n" in notices
    assert ".ci-licenses.json" not in notices
    assert "uv run pip-licenses" not in notices


@pytest.mark.parametrize("package", ["pip-licenses", "pip", "prettytable", "wcwidth"])
def test_policy_rejects_tooling_distribution_omitted_by_metadata_report(
    tmp_path: Path,
    package: str,
) -> None:
    root = _copy_policy_fixture(tmp_path)
    policy = root / "docs" / "research" / "dependency-license-policy.toml"
    _replace_once(policy, f'  "{package}==', f'  "omitted-{package}==')

    with pytest.raises(ValueError, match=rf"policy is missing.*{package}"):
        check_licenses.check_repository(root)


def test_policy_rejects_stale_dependency_not_present_in_frozen_inventory(tmp_path: Path) -> None:
    root = _copy_policy_fixture(tmp_path)
    policy = root / "docs" / "research" / "dependency-license-policy.toml"
    _replace_once(
        policy,
        '  "wcwidth==0.8.2",',
        '  "wcwidth==0.8.2",\n  "stale-package==1.0.0",',
    )

    with pytest.raises(ValueError, match=r"policy has stale.*stale-package"):
        check_licenses.check_repository(root)


def test_policy_rejects_partial_committed_lock_sbom_inventory(tmp_path: Path) -> None:
    root = _copy_policy_fixture(tmp_path)
    sbom = root / "sbom.cdx.json"
    document = json.loads(sbom.read_bytes())
    document["components"] = [
        component for component in document["components"] if component["name"] != "pip"
    ]
    sbom.write_text(json.dumps(document), encoding="utf-8")

    with pytest.raises(ValueError, match="frozen lock and committed Python SBOM"):
        check_licenses.check_repository(root)


def test_policy_rejects_partial_build_sbom_inventory(tmp_path: Path) -> None:
    root = _copy_policy_fixture(tmp_path)
    sbom = root / "sbom.build.cdx.json"
    document = json.loads(sbom.read_bytes())
    document["components"] = [
        component for component in document["components"] if component["name"] != "hatchling"
    ]
    sbom.write_text(json.dumps(document), encoding="utf-8")

    with pytest.raises(ValueError, match="build constraints and committed build SBOM"):
        check_licenses.check_repository(root)


def test_policy_rejects_missing_vendored_license_obligation(tmp_path: Path) -> None:
    root = _copy_policy_fixture(tmp_path)
    policy = root / "docs" / "research" / "dependency-license-policy.toml"
    _replace_once(
        policy,
        'obligations = ["preserve-license-files", "preserve-vendored-license-tree"]',
        'obligations = ["preserve-license-files"]',
    )

    with pytest.raises(ValueError, match=r"pip==26\.2\.1.*obligations"):
        check_licenses.check_repository(root)


def test_policy_rejects_new_external_tooling_exception(tmp_path: Path) -> None:
    root = _copy_policy_fixture(tmp_path)
    policy = root / "docs" / "research" / "dependency-license-policy.toml"
    _replace_once(
        policy,
        'classification = "compatible"\n'
        'obligations = ["preserve-license-files"]\n'
        'packages = [\n  "alembic==1.19.1",',
        'classification = "external-tooling-only"\n'
        'obligations = ["do-not-redistribute"]\n'
        'packages = [\n  "alembic==1.19.1",',
    )

    with pytest.raises(ValueError, match=r"external tooling exception.*alembic"):
        check_licenses.check_repository(root)


def test_policy_rejects_incomplete_mpl_source_obligation(tmp_path: Path) -> None:
    root = _copy_policy_fixture(tmp_path)
    policy = root / "docs" / "research" / "dependency-license-policy.toml"
    _replace_once(
        policy,
        'source_sha256 = "741e2c3b351ddf169a738da9f2c048608ff7f2c5cc02f1ebc6b118bb090d5d55"',
        'source_sha256 = ""',
    )

    with pytest.raises(ValueError, match=r"certifi==2026\.7\.22.*source SHA-256"):
        check_licenses.check_repository(root)


def test_policy_rejects_unreviewed_base_image_exception(tmp_path: Path) -> None:
    root = _copy_policy_fixture(tmp_path)
    policy = root / "docs" / "research" / "dependency-license-policy.toml"
    _replace_once(
        policy,
        'classification = "separately-licensed-base-image-components"',
        'classification = "unreviewed-exception"',
    )

    with pytest.raises(ValueError, match="base-image exception classification"):
        check_licenses.check_repository(root)


def test_policy_rejects_partial_base_image_inventory(tmp_path: Path) -> None:
    root = _copy_policy_fixture(tmp_path)
    sbom = root / "sbom.runtime-linux.cdx.json"
    document = json.loads(sbom.read_bytes())
    first_debian = next(
        index
        for index, component in enumerate(document["components"])
        if component["type"] == "operating-system"
    )
    del document["components"][first_debian]
    sbom.write_text(json.dumps(document), encoding="utf-8")

    with pytest.raises(ValueError, match="base-image package count changed"):
        check_licenses.check_repository(root)
