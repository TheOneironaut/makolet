from __future__ import annotations

import pytest

from makolet.adapters.persistence.destructive_target import (
    DestructiveDatabaseTargetError,
    require_benchmark_database_target,
    require_test_database_target,
)


@pytest.mark.parametrize("host", ["127.0.0.1", "localhost", "[::1]"])
def test_accepts_exact_confirmed_loopback_test_database(host: str) -> None:
    url = f"postgresql+asyncpg://makolet@{host}:55432/makolet_test_coverage"

    assert (
        require_test_database_target(
            url,
            confirmation="makolet_test_coverage",
        )
        == url
    )


def test_accepts_exact_confirmed_loopback_benchmark_database() -> None:
    url = "postgresql+asyncpg://makolet@127.0.0.1:55434/makolet_benchmark"

    assert (
        require_benchmark_database_target(
            url,
            confirmation="makolet_benchmark",
        )
        == url
    )


@pytest.mark.parametrize(
    ("url", "confirmation"),
    [
        ("postgresql://u@127.0.0.1/contest", "contest"),
        ("postgresql://u@127.0.0.1/latest", "latest"),
        ("postgresql://u@127.0.0.1/makolet", "makolet"),
        ("postgresql://u@203.0.113.10/makolet_test_coverage", "makolet_test_coverage"),
        (
            "postgresql://u@127.0.0.1/makolet_test_coverage?host=203.0.113.10",
            "makolet_test_coverage",
        ),
        (
            "postgresql://u@127.0.0.1/makolet_test_coverage?service=production",
            "makolet_test_coverage",
        ),
        (
            "postgresql://u@127.0.0.1/makolet_test_coverage?database=postgres",
            "makolet_test_coverage",
        ),
        (
            "postgresql://u@127.0.0.1/makolet_test_coverage?port=5432",
            "makolet_test_coverage",
        ),
        (
            "postgresql://u@127.0.0.1/makolet_test_coverage#?host=203.0.113.10",
            "makolet_test_coverage",
        ),
        ("postgresql://u@127.0.0.1/makolet_test_coverage", None),
        ("postgresql://u@127.0.0.1/makolet_test_coverage", "makolet_test_other"),
    ],
)
def test_rejects_ambiguous_or_unowned_test_database_targets(
    url: str,
    confirmation: str | None,
) -> None:
    with pytest.raises(DestructiveDatabaseTargetError):
        require_test_database_target(url, confirmation=confirmation)


@pytest.mark.parametrize(
    "url",
    [
        "postgresql://u@127.0.0.1/makolet_test_coverage",
        "postgresql://u@127.0.0.1/makolet_benchmark_other",
        "postgresql://u@db.example.test/makolet_benchmark",
    ],
)
def test_benchmark_requires_its_exact_local_database(url: str) -> None:
    with pytest.raises(DestructiveDatabaseTargetError):
        require_benchmark_database_target(url, confirmation="makolet_benchmark")
