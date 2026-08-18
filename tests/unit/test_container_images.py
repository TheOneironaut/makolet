from __future__ import annotations

import json
from pathlib import Path

import pytest

from scripts import check_container_images


def _image(reference: str, digest_character: str) -> dict[str, str]:
    return {
        "reference": reference,
        "digest": f"sha256:{digest_character * 64}",
        "license": "Apache-2.0",
        "source": "https://github.com/example/project",
    }


def _fixture(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    dockerfile_extra: str = "",
    compose_extra: str = "",
    extra_files: dict[str, str] | None = None,
) -> None:
    rows = (
        _image("example/frontend:1", "a"),
        _image("example/base:1", "b"),
        _image("example/service:1", "c"),
    )
    (tmp_path / "deployment").mkdir()
    (tmp_path / "deployment" / "container-images.lock").write_text(
        json.dumps({"schema_version": 1, "images": rows}),
        encoding="utf-8",
    )
    (tmp_path / "Dockerfile").write_text(
        "\n".join(
            (
                f"# syntax=example/frontend:1@sha256:{'a' * 64}",
                f"ARG BASE=example/base:1@sha256:{'b' * 64}",
                "FROM ${BASE} AS runtime",
                dockerfile_extra,
            )
        ),
        encoding="utf-8",
    )
    (tmp_path / "compose.yaml").write_text(
        "\n".join(
            (
                "x-app-service:",
                "  image: makolet:local",
                "  pull_policy: build",
                "  build:",
                "    context: .",
                "    dockerfile: Dockerfile",
                "services:",
                "  state:",
                f"    image: example/service:1@sha256:{'c' * 64}",
                compose_extra,
            )
        ),
        encoding="utf-8",
    )
    for relative_path, content in (extra_files or {}).items():
        target = tmp_path / relative_path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(content, encoding="utf-8")
    monkeypatch.setattr(check_container_images, "_ROOT", tmp_path)
    monkeypatch.setattr(
        check_container_images,
        "_LOCK",
        tmp_path / "deployment" / "container-images.lock",
    )


def test_accepts_exact_bidirectional_image_inventory(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _fixture(tmp_path, monkeypatch)

    assert check_container_images.main() == 0


@pytest.mark.parametrize(
    ("dockerfile_extra", "compose_extra"),
    [
        (f"FROM example/unlisted:1@sha256:{'d' * 64}", ""),
        ("COPY --from=example/unlisted:latest /bin/tool /bin/tool", ""),
        ("RUN --mount=type=bind,from=example/unlisted:latest true", ""),
        ("", f"  extra:\n    image: example/unlisted:1@sha256:{'d' * 64}"),
        ("", "  extra:\n    image: example/unlisted:1"),
        ("", "  extra: { image: example/unlisted:latest }"),
        ("", '  extra:\n    "image": example/unlisted:latest'),
        ("", '  extra:\n    "\\u0069mage": example/unlisted:latest'),
        ("", "  extra:\n    build:\n      args:\n        BASE: example/unlisted:latest"),
        ("", "  hidden: { build: ./alternate }"),
        ("", '  hidden: { "\\u0069mage": example/unlisted:latest }'),
        ("", '  hidden: ! { "\\u0069mage": example/unlisted:latest }'),
        ("", "  !!str image: example/unlisted:latest"),
        ("", '  !!str "image": example/unlisted:latest'),
        ("", "  &key image: example/unlisted:latest"),
        ("", "  ? image\n  : example/unlisted:latest"),
        ("", "  !!str build: ./alternate"),
    ],
)
def test_rejects_every_unlisted_or_floating_configured_image(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    dockerfile_extra: str,
    compose_extra: str,
) -> None:
    _fixture(
        tmp_path,
        monkeypatch,
        dockerfile_extra=dockerfile_extra,
        compose_extra=compose_extra,
    )

    assert check_container_images.main() == 1


def test_repository_image_inventory_is_exact() -> None:
    assert check_container_images.main() == 0


def test_rejects_local_image_exception_without_a_sibling_build(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _fixture(
        tmp_path,
        monkeypatch,
        compose_extra=(
            "  unbuilt:\n"
            "    image: makolet:local\n"
            "    pull_policy: build\n"
            '    command: ["makolet", "--help"]'
        ),
    )

    assert check_container_images.main() == 1


@pytest.mark.parametrize(
    "compose_extra",
    [
        "  floating-local:\n    image: ${MAKOLET_IMAGE:-makolet:local}\n    build: .",
        "  pullable-local:\n    image: makolet:local\n    build: .",
        "  never-build-local:\n    image: makolet:local\n    pull_policy: never\n    build: .",
    ],
)
def test_rejects_overridable_or_not_forced_local_application_images(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    compose_extra: str,
) -> None:
    _fixture(tmp_path, monkeypatch, compose_extra=compose_extra)

    assert check_container_images.main() == 1


def test_rejects_unlisted_image_in_referenced_dockerfile(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _fixture(
        tmp_path,
        monkeypatch,
        compose_extra=(
            "  alternate:\n"
            "    image: makolet:local\n"
            "    pull_policy: build\n"
            "    build:\n"
            "      context: .\n"
            "      dockerfile: containers/Alternate"
        ),
        extra_files={
            "containers/Alternate": "FROM example/unlisted:latest\n",
        },
    )

    assert check_container_images.main() == 1


def test_resolves_dockerfile_below_its_build_context(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _fixture(
        tmp_path,
        monkeypatch,
        compose_extra=(
            "  alternate:\n"
            "    image: makolet:local\n"
            "    pull_policy: build\n"
            "    build:\n"
            "      context: alternate\n"
            "      dockerfile: Dockerfile"
        ),
        extra_files={
            "alternate/Dockerfile": "FROM example/unlisted:latest\n",
        },
    )

    assert check_container_images.main() == 1


@pytest.mark.parametrize(
    "override_name",
    [
        "compose.override.yaml",
        "compose.override.yml",
        "docker-compose.override.yaml",
        "docker-compose.override.yml",
    ],
)
def test_rejects_default_compose_override_files(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    override_name: str,
) -> None:
    _fixture(tmp_path, monkeypatch)
    (tmp_path / override_name).write_text(
        "services: { api: { image: example/unlisted:latest } }\n",
        encoding="utf-8",
    )

    assert check_container_images.main() == 1


def test_rejects_home_expanding_build_context(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _fixture(
        tmp_path,
        monkeypatch,
        compose_extra=(
            "  alternate:\n"
            "    image: makolet:local\n"
            "    pull_policy: build\n"
            "    build:\n"
            "      context: ~/alternate\n"
            "      dockerfile: Dockerfile"
        ),
    )

    assert check_container_images.main() == 1


@pytest.mark.parametrize(
    "dockerfile",
    [
        "# syntax = example/unlisted:latest\nFROM scratch\n",
        (f"\ufeff# syntax=example/unlisted:latest\nFROM example/base:1@sha256:{'b' * 64}\n"),
        (
            f"# syntax=example/frontend:1@sha256:{'a' * 64}\n"
            f"FROM example/base:1@sha256:{'b' * 64} AS runtime\n"
            "RUN echo \\\\\n"
            "FROM example/unlisted:latest AS hidden\n"
        ),
        (
            f"# syntax=example/frontend:1@sha256:{'a' * 64}\n"
            "# escape=`\n"
            "FROM example/unlisted:`\n"
            "latest AS runtime\n"
        ),
        (
            f"# syntax=example/frontend:1@sha256:{'a' * 64}\n"
            "# escape=`\n"
            f"FROM example/base:1@sha256:{'b' * 64} AS runtime\n"
            "COPY --from=example/unlisted:`\n"
            "latest /bin/tool /bin/tool\n"
        ),
    ],
)
def test_rejects_spaced_or_custom_escape_dockerfile_bypasses(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    dockerfile: str,
) -> None:
    _fixture(tmp_path, monkeypatch)
    (tmp_path / "Dockerfile").write_text(dockerfile, encoding="utf-8")

    assert check_container_images.main() == 1
