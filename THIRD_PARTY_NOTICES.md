# Third-party notices

No source code, documentation, fixtures, data, images, or other copyrightable
material from the research repositories in the ledger below has been incorporated
into Makolet. The clean-room statement does not mean installed Python dependencies
are authored by Makolet: they are separately licensed packages resolved from PyPI.

The repository does not vendor dependency wheels. If a release distributes a
container, wheelhouse, executable bundle, virtual environment, or other artifact
that contains them, it must include the complete upstream license and notice files
described below. This summary is not a substitute for those files.

## Runtime, development, security, and build dependencies

Snapshot: 2026-08-12, CPython 3.14.7, `uv` 0.12.3. The complete
84-record locked ledger, exact six-package build closure, Windows and pinned
Linux artifact evidence, hashes, rejected-artifact history, and reproducible
commands are in
[`docs/research/dependency-license-audit.md`](docs/research/dependency-license-audit.md).

### Compatibility decision

The two confirmed dependency-policy blockers have been removed from the selected
graph:

- `pyarrow==25.0.1` was removed from the manifest, lock, SBOM, and environment.
- Unused `mcp==2.0.0` and `testcontainers==4.15.0` were removed. They were the
  only paths to Windows `pywin32==312` and its LGPL `adodbapi` payload.
  `pip-audit` and `pip-licenses` remain pinned because their closures do not
  select `pywin32`.

`uv.lock` and the synchronized Windows environment contain Makolet plus 84
third-party PyPI records. Marker-aware exports contain 42 runtime, 28
development, and 31 security records, with overlap. The pinned Linux runtime
contains 41 third-party Python distributions (`colorama` is Windows-only),
CPython, and 97 Debian packages.

At the package-license layer, the current union has no identified GPL, LGPL,
AGPL, proprietary, custom, unknown-license, or non-OSI package. Its exclusive
summary buckets are 19 Apache-family, 15 BSD-family, 45 MIT-family, one ISC, two
PSF-2.0, and two MPL-2.0 records.

`certifi==2026.7.22` and `pathspec==1.1.1` are OSI-approved MPL-2.0 weak
copyleft. Project policy permits their unmodified artifacts alongside Makolet's
Apache-2.0 files. Distribution requires prominent labeling, the MPL text, an
exact-source availability path, and MPL-2.0 availability of covered-file
modifications. MPL does not extend to separate Makolet files.

Actual `pip-audit==2.10.1` scans of the exact all-groups export and exact
hash-pinned build closure reported no known vulnerabilities from the PyPI
advisory service on the audit date. This is a point-in-time result, not a
guarantee.

The committed runtime SBOM covers the exact pinned Linux image, including its
native links and Debian source-package identities. Debian base-OS programs and
libraries include GPL/LGPL terms even though the selected Python graph does not;
container redistribution must satisfy their notices and corresponding-source
conditions. A future bundled Windows runtime needs its own artifact audit.

### Exact direct runtime pins

| Direct dependency and source | License/SPDX conclusion | Notice / decision |
|---|---|---|
| [`alembic==1.19.1`](https://pypi.org/project/alembic/1.19.1/) | MIT | Retain MIT notice. |
| [`anyio==4.14.2`](https://pypi.org/project/anyio/4.14.2/) | MIT | Retain MIT notice. |
| [`asyncpg==0.31.0`](https://pypi.org/project/asyncpg/0.31.0/) | Apache-2.0 | Retain license; native wheel. |
| [`boto3==1.43.68`](https://pypi.org/project/boto3/1.43.68/) | Apache-2.0 | Preserve license and `NOTICE`. |
| [`fastapi==0.141.1`](https://pypi.org/project/fastapi/0.141.1/) | MIT | Retain MIT notice. |
| [`httpx==0.28.1`](https://pypi.org/project/httpx/0.28.1/) | BSD-3-Clause | Retain BSD notice. |
| [`prometheus-client==0.26.0`](https://pypi.org/project/prometheus-client/0.26.0/) | Apache-2.0 AND BSD-2-Clause | Preserve both licenses and `NOTICE`. |
| [`pydantic==2.13.4`](https://pypi.org/project/pydantic/2.13.4/) | MIT | Retain MIT notice. |
| [`pydantic-settings==2.15.0`](https://pypi.org/project/pydantic-settings/2.15.0/) | MIT | Retain MIT notice. |
| [`rich==15.0.0`](https://pypi.org/project/rich/15.0.0/) | MIT | Retain MIT notice. |
| [`sqlalchemy[asyncio]==2.0.51`](https://pypi.org/project/SQLAlchemy/2.0.51/) | MIT | Retain MIT notice; native wheel. |
| [`structlog==26.1.0`](https://pypi.org/project/structlog/26.1.0/) | MIT OR Apache-2.0 | MIT branch selected; preserve upstream `NOTICE`. |
| [`tenacity==9.1.4`](https://pypi.org/project/tenacity/9.1.4/) | Apache-2.0 | Retain license. |
| [`typer==0.27.1`](https://pypi.org/project/typer/0.27.1/) | MIT | Retain MIT notice. |
| [`tzdata==2026.3`](https://pypi.org/project/tzdata/2026.3/) | Apache-2.0 | Preserve packaged `LICENSE` and `LICENSE_APACHE`; contains compiled IANA data. |
| [`uvicorn==0.52.1`](https://pypi.org/project/uvicorn/0.52.1/) | BSD-3-Clause | Retain BSD notice; verified with asyncio and `h11`, without the `standard` extra. |

### Exact direct development and security pins

| Scope | Direct dependency and source | License/SPDX conclusion | Notice / decision |
|---|---|---|---|
| Development | [`mypy==2.3.0`](https://pypi.org/project/mypy/2.3.0/) | MIT AND PSF-2.0 AND Apache-2.0 | Preserve all shipped licenses; native wheel. |
| Development | [`psutil==7.2.2`](https://pypi.org/project/psutil/7.2.2/) | BSD-3-Clause | Retain BSD notice; native wheel. |
| Development | [`pytest==9.1.1`](https://pypi.org/project/pytest/9.1.1/) | MIT | Retain MIT notice. |
| Development | [`pytest-asyncio==1.4.0`](https://pypi.org/project/pytest-asyncio/1.4.0/) | Apache-2.0 | Retain license. |
| Development | [`pytest-cov==7.1.0`](https://pypi.org/project/pytest-cov/7.1.0/) | MIT | Retain MIT notice. |
| Development | [`respx==0.23.1`](https://pypi.org/project/respx/0.23.1/) | BSD-3-Clause | Retain BSD notice. |
| Development | [`ruff==0.16.2`](https://pypi.org/project/ruff/0.16.2/) | MIT package; embedded CC0-1.0 and WTFPL | Repository-mandated external tooling exception only; do not redistribute with Makolet. |
| Development | [`types-boto3[s3]==1.43.68`](https://pypi.org/project/types-boto3/1.43.68/) | MIT | Retain MIT notice. |
| Security | [`pip-audit==2.10.1`](https://pypi.org/project/pip-audit/2.10.1/) | Apache-2.0 | Retain license; no `pywin32` edge. |
| Security | [`pip-licenses==5.5.5`](https://pypi.org/project/pip-licenses/5.5.5/) | MIT | Retain MIT notice; no `pywin32` edge. |

The isolated build closure is separate from `uv.lock` and exact-hash constrained
in `build-constraints.txt`: `hatchling==1.32.0` (MIT), `packaging==26.3`
(Apache-2.0 OR BSD-2-Clause), `pathspec==1.1.1` (MPL-2.0), `pluggy==1.6.0`
(MIT), `tomlkit==0.15.1` (MIT), and `trove-classifiers==2026.6.1.19`
(Apache-2.0). `sbom.build.cdx.json` records both distribution artifacts and
hashes for all six; a fresh isolated wheel and sdist build passed with
`--require-hashes`.

### Transitive license and notice summary

The complete exact package list is in the detailed audit. Notice-bearing or
multi-license distributions requiring special preservation include `boto3`,
`botocore`, `coverage`, `cyclonedx-python-lib`, `license-expression`,
`prometheus-client`, `requests`, `s3transfer`, `structlog`, `pip` (its complete
vendored-license tree), and `ast-serialize` (root and Rust-crate licenses).

Windows native payloads include `ast-serialize`, `asyncpg`,
`charset-normalizer`, `coverage`, `greenlet`, `librt`, `MarkupSafe`, `msgpack`,
`mypy`, `psutil`, `pydantic-core`, `ruff`, `SQLAlchemy`, and `tomli`. PE imports
resolve only to Python and Windows/MSVC platform DLLs; no additional bundled DLL
was found. The exact Linux runtime has 13 ELF extensions across `asyncpg`,
`greenlet`, `MarkupSafe`, `pydantic-core`, and `SQLAlchemy`; all dynamic links
resolve to inventoried base-image libraries. `pydantic-core`'s 103-component
embedded SBOM supplies its static Rust provenance.

Ruff's 335-component embedded SBOM identifies CC0-only `notify==8.2.0` and
WTFPL `terminfo==0.9.0`. Ruff is absent from the product wheel and runtime image
and must remain an externally obtained development/CI tool. The other installed
embedded SBOMs (`ast-serialize`, 91 components; `pydantic-core`, 103 components)
did not expose a CC0-only or WTFPL component. `pydantic-core`'s
`r-efi==5.2.0` permits MIT or Apache-2.0, which is the selected branch rather
than its LGPL alternative.

### Rejected artifacts are not current dependencies

- The rejected PyArrow 25.0.1 Windows wheel had Apache-2.0 metadata but bundled
  two Microsoft Visual C++ runtime DLLs without their proprietary redistribution
  terms, statically embedded bzip2 1.0.8 without its non-OSI notice, contained
  CC0 material, and lacked complete static-component evidence. It remains
  prohibited and is no longer selected.
- `pywin32==312` included `adodbapi` under `LGPL-2.1-or-later` plus MIT and
  custom portions. It remains prohibited and is no longer selected.
- Current [`duckdb==1.5.5`](https://pypi.org/project/duckdb/1.5.5/) can write
  partitioned Parquet, but its Windows wheel statically links ICU and other
  components while omitting their license/notice payload. It also statically
  incorporates the Microsoft C runtime without carrying Microsoft's
  proprietary/non-OSI terms. ICU's custom IPADIC/ICOT data terms and
  public-domain data independently fail the literal OSI-only gate. DuckDB is on
  hold and was not added.
- [`fastparquet==2026.5.0`](https://pypi.org/project/fastparquet/2026.5.0/)
  is Apache-2.0 but its project says pandas 3.0 broke it and it is being retired;
  its nine-package closure includes pandas and NumPy. It was not added.

The current lock deliberately has no third-party Parquet engine. Makolet's
independently written, bounded writer is implemented and its output was read by
isolated DuckDB 1.5.4. The Parquet requirement is therefore closed without a
license exception or a selected third-party engine; see
[`docs/research/parquet-interop.md`](docs/research/parquet-interop.md).

### Required distribution actions

1. Preserve every distributed package's complete `LICENSE*`, `COPYING*`,
   `NOTICE*`, `AUTHORS*`, and embedded SBOM files. Propagate applicable
   Apache, MIT, BSD, ISC, and PSF attribution text.
2. For `certifi` and `pathspec`, prominently label MPL-2.0 inclusion, include
   the MPL text, provide an exact-source path, and publish covered-file
   modifications under MPL-2.0.
3. Preserve the special notice and vendored-license bundles named above and both
   packaged `tzdata` license files.
4. Do not redistribute Ruff. Its external development/CI use is the only
   exception for its embedded CC0-only and WTFPL components.
5. Do not reintroduce PyArrow, pywin32, DuckDB's current PyPI wheel, Fastparquet,
   or another Parquet engine without a fresh exact-artifact audit.
6. For the pinned Linux image, retain the runtime SBOM and Debian copyright
   evidence and satisfy each GPL/LGPL package's corresponding-source terms. If
   a Windows runtime is bundled, audit its exact CPython/MSVC/platform artifacts.

Exact MPL-covered source for this snapshot:

- `certifi==2026.7.22`: [source archive](https://files.pythonhosted.org/packages/a3/c2/24167ea9858356b47a87a50d39908bfdb72ceeefe0041586e704e5376b3a/certifi-2026.7.22.tar.gz), SHA-256 `741e2c3b351ddf169a738da9f2c048608ff7f2c5cc02f1ebc6b118bb090d5d55`;
- `pathspec==1.1.1`: [source archive](https://files.pythonhosted.org/packages/5a/82/42f767fc1c1143d6fd36efb827202a2d997a375e160a71eb2888a925aac1/pathspec-1.1.1.tar.gz), SHA-256 `17db5ecd524104a120e173814c90367a96a98d07c45b2e10c2f3919fff91bf5a`.

### Verification commands and result

```powershell
uv lock --check
uv sync --locked --all-groups --dry-run
uv pip check --python .venv\Scripts\python.exe
$req = New-TemporaryFile
uv export --frozen --all-groups --no-emit-project --format requirements-txt --output-file $req
uv run pip-audit --requirement $req --no-deps --disable-pip --progress-spinner off --strict
uv run pip-audit --requirement build-constraints.txt --no-deps --disable-pip --progress-spinner off --strict
uv run python scripts/check_licenses.py
uv run python scripts/check_build_constraints.py
uv run python scripts/check_distribution.py --reproducible
uv export --preview-features sbom-export --frozen --all-groups --format cyclonedx1.5 --output-file .ci-sbom.json
uv run python scripts/check_sbom.py sbom.cdx.json .ci-sbom.json
uv run python scripts/check_container_images.py
docker build --tag makolet:license-audit .
docker run --rm --user 10001:10001 --volume "${PWD}:/audit" --entrypoint python makolet:license-audit /audit/scripts/generate_runtime_sbom.py --output /audit/.ci-runtime-sbom.json
uv run python scripts/check_sbom.py --runtime-semantic sbom.runtime-linux.cdx.json .ci-runtime-sbom.json
Get-FileHash pyproject.toml,uv.lock,sbom.cdx.json,sbom.build.cdx.json,sbom.runtime-linux.cdx.json,build-constraints.txt -Algorithm SHA256
Remove-Item -LiteralPath $req
```

The lock/sync/environment checks passed; both vulnerability audits reported no
known vulnerabilities; the frozen dependency inventory passed the reviewed,
fail-closed license/notice policy; the
build was reproducible from the hash-pinned closure; and the main, build, and
Linux runtime SBOM checks passed. Remaining obligations are artifact-specific:
do not redistribute Ruff, comply with the exact MPL and Debian terms, and audit
any future Windows bundle or changed image/dependency artifact.

The research ledger below is not a runtime dependency notice and does not approve
any research package for installation. In particular, researched scraper and
parser repositories with a custom non-commercial license must not be added as
dependencies.

## Research-only repository ledger -- no material incorporated

These repositories were inspected to understand publicly documented behavior,
identify official-source research leads, and audit possible licensing constraints.
No material from any row has been incorporated. Detailed evidence, learned concepts,
and decisions are in [`docs/research/open-source-review.md`](docs/research/open-source-review.md).

| Research repository | Pinned revision and date (UTC) | License/SPDX conclusion | Incorporation and notice status |
|---|---|---|---|
| [OpenIsraeliSupermarkets/israeli-supermarket-scarpers](https://github.com/OpenIsraeliSupermarkets/israeli-supermarket-scarpers) | [`d34a32ba5bf1630028da68b00702d1f2ab6b2dcc`](https://github.com/OpenIsraeliSupermarkets/israeli-supermarket-scarpers/commit/d34a32ba5bf1630028da68b00702d1f2ab6b2dcc), 2026-08-05 16:35:23Z | Custom non-commercial; GitHub `NOASSERTION`; `LicenseRef-OpenIsraeliSupermarkets-Custom-NC` | Research only; incompatible with Apache-2.0; no material incorporated. Permitted non-commercial reuse would require naming Sefi Erlich, linking the repository, and identifying changes, but Makolet does not rely on that permission. |
| [OpenIsraeliSupermarkets/daily-publish-supermarket-data](https://github.com/OpenIsraeliSupermarkets/daily-publish-supermarket-data) | [`56e4014c0857018baad3023243d2dfeda4262270`](https://github.com/OpenIsraeliSupermarkets/daily-publish-supermarket-data/commit/56e4014c0857018baad3023243d2dfeda4262270), 2026-08-08 17:15:37Z | Custom non-commercial; GitHub `NOASSERTION`; `LicenseRef-OpenIsraeliSupermarkets-Custom-NC` | Research only; incompatible; no material incorporated. Same Sefi Erlich/link/change attribution terms for otherwise permitted non-commercial use. |
| [OpenIsraeliSupermarkets/product-matching-service](https://github.com/OpenIsraeliSupermarkets/product-matching-service) | [`d64c70aa38dffc16cc5b538472ee743b8e014c76`](https://github.com/OpenIsraeliSupermarkets/product-matching-service/commit/d64c70aa38dffc16cc5b538472ee743b8e014c76), 2026-06-06 18:23:36Z | No license; SPDX conclusion `NONE` | Research only; no redistribution permission and no material incorporated. |
| [AKorets/israeli-supermarket-data](https://github.com/AKorets/israeli-supermarket-data) | [`a2ac4df3861bba202315e89aeb603abe55a946d4`](https://github.com/AKorets/israeli-supermarket-data/commit/a2ac4df3861bba202315e89aeb603abe55a946d4), 2023-08-17 12:58:38Z | MIT, SPDX `MIT`; copyright 2023 Avram Korets | Compatible in principle, but research only and no material incorporated. Future reuse must retain the copyright and MIT permission notice. |
| [amichai1/israeli-price-comparison](https://github.com/amichai1/israeli-price-comparison) | [`4f103c5a88623f274019f4f5809a45390e60107e`](https://github.com/amichai1/israeli-price-comparison/commit/4f103c5a88623f274019f4f5809a45390e60107e), 2026-02-25 19:50:55Z | No repository-wide license; SPDX conclusion `NONE`; nested scraper manifest says `MIT` with unclear scope | Research only pending upstream clarification; no material incorporated. |
| [OpenIsraeliSupermarkets/israeli-supermarket-parsers](https://github.com/OpenIsraeliSupermarkets/israeli-supermarket-parsers) | [`346ebeace8d0ef7700162339836ae18039f1ae8c`](https://github.com/OpenIsraeliSupermarkets/israeli-supermarket-parsers/commit/346ebeace8d0ef7700162339836ae18039f1ae8c), 2026-08-08 21:10:41Z | Custom non-commercial; GitHub `NOASSERTION`; `LicenseRef-OpenIsraeliSupermarkets-Custom-NC` | Research only; incompatible; no material incorporated. Same Sefi Erlich/link/change attribution terms for otherwise permitted non-commercial use. |
| [nitaiaharoni1/super-mcp](https://github.com/nitaiaharoni1/super-mcp) | [`0f0c901247e87a18f4ffc48ea2550c261c18ebaa`](https://github.com/nitaiaharoni1/super-mcp/commit/0f0c901247e87a18f4ffc48ea2550c261c18ebaa), 2026-08-11 13:59:27Z | Apache License 2.0, SPDX `Apache-2.0` | Compatible in principle, but research only and no material incorporated. Future reuse must include the license, mark modified files, retain notices, and propagate applicable `NOTICE` content; no upstream `NOTICE` existed at this revision. |
| [Segalil/israeli-supermarket-prices](https://github.com/Segalil/israeli-supermarket-prices) | [`a858e9c31193637830a997538668bfcdc2a2f055`](https://github.com/Segalil/israeli-supermarket-prices/commit/a858e9c31193637830a997538668bfcdc2a2f055), 2026-08-11 17:23:41Z | MIT, SPDX `MIT`; copyright 2026 Segalil | Its original code is compatible in principle, but research only and no material incorporated. Future reuse must retain the MIT notice and must not inherit its custom-NC scraper dependency. |

This ledger records research provenance only. It creates no claim that a software
license covers retailer files, generated datasets, Kaggle content, fixtures, product
images, trademarks, or other third-party material.
