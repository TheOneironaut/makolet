from __future__ import annotations

from makolet.adapters.persistence.ingestion import _is_signed_url_rotation


def test_signed_url_rotation_changes_only_ephemeral_query_evidence() -> None:
    original = "https://cdn.example/catalog/file.xml?tenant=public&se=2026-08-17&sig=first"

    assert _is_signed_url_rotation(
        original,
        "https://cdn.example/catalog/file.xml?tenant=public&se=2026-08-18&sig=second",
    )
    assert not _is_signed_url_rotation(
        original,
        "https://cdn.example/catalog/other.xml?tenant=public&se=2026-08-18&sig=second",
    )
    assert not _is_signed_url_rotation(
        original,
        "https://cdn.example/catalog/file.xml?tenant=private&se=2026-08-18&sig=second",
    )
    assert not _is_signed_url_rotation(
        "https://cdn.example/catalog/file.xml?version=one",
        "https://cdn.example/catalog/file.xml?version=two",
    )
