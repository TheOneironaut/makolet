"""Prepare and validate an isolated database before a restore-time swap."""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
from collections.abc import Mapping, Sequence

from sqlalchemy.engine import make_url

_STAGING_DATABASE = re.compile(r"makolet_restore_[0-9]{14}_[0-9]+\Z")
_MIGRATION_REVISION = re.compile(r"[A-Za-z0-9_-]{1,64}\Z")


def prepare_staging_database(database_name: str, base_database_url: str) -> str:
    """Migrate an isolated restore target and return its verified head label."""
    if _STAGING_DATABASE.fullmatch(database_name) is None:
        raise ValueError("restore staging database name is invalid")
    base_url = make_url(base_database_url)
    if base_url.database == database_name:
        raise ValueError("restore staging database must differ from the active database")
    staging_url = base_url.set(database=database_name).render_as_string(hide_password=False)
    environment = os.environ.copy()
    environment["MAKOLET_DATABASE_URL"] = staging_url

    migration = subprocess.run(
        (sys.executable, "-m", "makolet.interfaces.cli", "database", "migrate", "--json"),
        check=False,
        env=environment,
        stdout=subprocess.DEVNULL,
    )
    if migration.returncode != 0:
        raise RuntimeError("staging database migration failed")

    status_process = subprocess.run(
        (sys.executable, "-m", "makolet.interfaces.cli", "database", "status", "--json"),
        check=False,
        env=environment,
        stdout=subprocess.PIPE,
        text=True,
    )
    if status_process.returncode != 0:
        raise RuntimeError("staging database status failed")
    try:
        status = json.loads(status_process.stdout)
    except json.JSONDecodeError as error:
        raise RuntimeError("staging database status was not valid JSON") from error
    if not isinstance(status, dict):
        raise TypeError("staging database status was not an object")
    return _verified_head_label(status)


def _verified_head_label(status: Mapping[str, object]) -> str:
    expected = _revision_list(status.get("expected_migration_heads"))
    current = _revision_list(status.get("current_migration_revisions"))
    if status.get("schema_ready") is not True or not current or current != expected:
        raise RuntimeError("staging database is not exactly at the expected migration head")
    return ",".join(current)


def _revision_list(value: object) -> tuple[str, ...]:
    if not isinstance(value, Sequence) or isinstance(value, str | bytes | bytearray):
        raise TypeError("database status migration revisions are invalid")
    revisions = tuple(value)
    if any(
        not isinstance(revision, str) or _MIGRATION_REVISION.fullmatch(revision) is None
        for revision in revisions
    ):
        raise RuntimeError("database status migration revisions are invalid")
    return tuple(sorted(revisions))


def main() -> int:
    database_name = os.environ.get("MAKOLET_RESTORE_STAGING_DATABASE", "")
    database_url = os.environ.get("MAKOLET_DATABASE_URL", "")
    try:
        revision = prepare_staging_database(database_name, database_url)
    except Exception:
        sys.stderr.write(
            "Staging database migration and head verification failed; "
            "the active database is unchanged\n"
        )
        return 1
    sys.stdout.write(f"{revision}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
