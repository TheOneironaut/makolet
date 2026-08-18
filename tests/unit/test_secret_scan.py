"""Regression tests for the dependency-free repository secret scanner."""

from scripts import check_secrets


def test_secret_scan_checks_every_assignment_candidate() -> None:
    line = "PASSWORD=placeholder; API_KEY=production-value-123456"  # secret-scan: allow

    assert check_secrets._scan_line(line) == ["credential-assignment"]


def test_secret_scan_checks_every_credential_uri_candidate() -> None:
    line = (
        "postgresql://user:placeholder@localhost/first "
        "postgresql://user:production-value-123456@localhost/second"  # secret-scan: allow
    )

    assert check_secrets._scan_line(line) == ["credential-in-uri"]


def test_secret_scan_accepts_multiple_documented_placeholders() -> None:
    line = "PASSWORD=placeholder; API_KEY=example-secret"

    assert check_secrets._scan_line(line) == []


def test_secret_scan_rejects_uri_valued_and_prefixed_credentials() -> None:
    assert check_secrets._scan_line(
        "PASSWORD=https://example.invalid/production-credential-9f3b1c2d"  # secret-scan: allow
    ) == ["credential-assignment"]
    assert check_secrets._scan_line(
        "AWS_SECRET_ACCESS_KEY=foo://production-credential-9f3b1c2d"  # secret-scan: allow
    ) == ["credential-assignment"]
    assert check_secrets._scan_line(
        "TOKEN=contest-exampled-production-credential-9f3b1c2d"  # secret-scan: allow
    ) == ["credential-assignment"]
    assert check_secrets._scan_line(
        "MAKOLET_S3_SECRET_KEY=production-credential-9f3b1c2d"  # secret-scan: allow
    ) == ["credential-assignment"]


def test_secret_scan_keeps_documented_placeholders_and_non_credential_fields() -> None:
    assert check_secrets._scan_line("PASSWORD=placeholder; API_KEY=example-secret") == []
    assert check_secrets._scan_line("- password_encryption=scram-sha-256") == []
    assert check_secrets._scan_line('aws_secret_access_key="test-secret-key"') == []
