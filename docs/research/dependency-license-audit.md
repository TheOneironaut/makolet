# Resolved dependency license and vulnerability audit

Audit date: 2026-08-12; package-manifest refresh: 2026-08-17 (UTC+03:00)

This is an engineering compliance record, not legal advice. It distinguishes
declared Python-package licenses from the contents of platform wheels and does
not infer redistribution permission from package metadata alone.

## Result

The selected Python product and build graphs now have exact, auditable closures:

- `pyarrow==25.0.1`, unused `mcp==2.0.0`, and unused
  `testcontainers==4.15.0` remain removed. Their rejected-artifact evidence is
  retained below. There is no selected `pywin32` or `adodbapi` edge.
- `uvicorn[standard]==0.52.1` was narrowed to `uvicorn==0.52.1`. Makolet uses
  Uvicorn's asyncio and `h11` path and does not need the extra's `httptools`,
  `PyYAML`, `uvloop`, `watchfiles`, or `websockets` packages. This also removes
  `watchfiles`' statically incorporated CC0-only `notify` component from the
  product graph.
- Hatchling's complete isolated-build closure is exact-version and
  distribution-hash constrained in `build-constraints.txt`, represented by the
  six-component `sbom.build.cdx.json`, vulnerability-audited, and exercised by a
  fresh isolated wheel and sdist build with hash enforcement.
- [`dependency-license-policy.toml`](dependency-license-policy.toml) is the
  machine-readable reviewed classification and obligation ledger. The offline
  `scripts/check_licenses.py` gate requires an exact set equality across all 84
  third-party `uv.lock` records, the lock's build-constraint manifest, the six
  hash-constrained build distributions, and both committed Python SBOM inventories.
  The three overlapping build packages yield 87 unique reviewed Python identities.
  Missing, stale, duplicate, partially inventoried, or unclassified records fail.
- The exact pinned Linux runtime image is represented by
  `sbom.runtime-linux.cdx.json`: 41 Python distributions, CPython, and 97 Debian
  binary packages, including machine-readable license labels, license/copyright
  evidence hashes, Debian source package identities, and native dynamic-link
  evidence.
- The dependency-free bounded Parquet writer is implemented. Its output was
  independently read with isolated DuckDB 1.5.4 as recorded in
  [`parquet-interop.md`](parquet-interop.md); no rejected Parquet engine was
  added to the product.

At the package-license layer, the 84-record locked Python union contains no
identified GPL, LGPL, AGPL, proprietary, custom, unknown, or other non-OSI
package. `certifi==2026.7.22` and build/development dependency
`pathspec==1.1.1` are OSI-approved MPL-2.0. Project policy permits their
unmodified artifacts under the file-level obligations stated below; this is not
a blanket Category-B exception.

On 2026-08-16 the owner explicitly approved one narrowly scoped tooling exception
for `ruff==0.16.2`. Ruff is
mandated by the repository agent guide and used only as an external development
and CI executable. Its embedded 335-component SBOM identifies CC0-only
`notify==8.2.0` and WTFPL `terminfo==0.9.0`, neither of which is an OSI-approved
software license. Ruff is not a runtime dependency, is absent from the product
wheel and Linux runtime image, and must not be redistributed by Makolet. This
exception does not relax the product/build dependency rule. The other installed
PEP 770 component inventories (`ast-serialize` and `pydantic-core`) did not
identify the same issue; their alternative-license selections are recorded
below.

The same owner decision explicitly permits separately licensed base-OS and service
components, including the Debian operating-system components in the runtime image.
Some are GPL/LGPL licensed even though the selected Python graph is not. They remain
separate platform programs or dynamically linked system libraries, do not
relicense Makolet, and are inventoried with exact Debian source versions and
machine-readable license names bound to copyright-file hashes. A container
redistributor must satisfy each package's notice and corresponding-source terms.
The approval does not incorporate those components into Makolet's Apache-2.0 source
or waive their notices, source-availability, or corresponding-source obligations.

## Reproducible snapshot

| Item | Verified value |
|---|---|
| Host | Windows 11 `10.0.26200`, AMD64 |
| Python | CPython 3.14.7, MSC v.1944, 64-bit |
| uv | 0.12.3 (`507230998`, `x86_64-pc-windows-msvc`) |
| `pyproject.toml` SHA-256 | `D51C3D00E8169BDD794616D000E1D5A6E62F6FC34F2099F5B81294CA4BE2D5ED` |
| `uv.lock` SHA-256 | `CA85F61FFEB5F90D65945579B20C86982489C87C68371C75057EBF697ED2079F` |
| `sbom.cdx.json` SHA-256 | `56D6DBEE41F7665D22BE6DD482D9893CE0D15257E2AD3A77EC3E4F8EBF707EF6` |
| `build-constraints.txt` SHA-256 | `1053AB3B4BB17117E5C3DC163BF450D366A199D0FC35461D334FE3AF903DA24D` |
| `sbom.build.cdx.json` SHA-256 | `16E816197EA19185B5DDE6C4AAED0F490E918DB7E5F62D46762848D002951250` |
| `sbom.runtime-linux.cdx.json` SHA-256 | `D6A06D38BD3BFDD868299ADD1473DE06A160AA1D216A02B0B375BB4C39B66508` |
| `dependency-license-policy.toml` SHA-256 | `0260B0798B7D186AE9E48D9A9BE2E461A3CFEB25706D6506722A9CFA5AED5E7F` |
| Lock format | version 1, revision 3, Python `>=3.14.7,<3.15` |
| Lock inventory | Makolet plus 84 third-party PyPI records |
| Windows environment | Makolet plus 84 third-party distributions |
| Main SBOM inventory | 84 third-party CycloneDX components plus Makolet as root metadata |
| Marker-aware scopes | 42 runtime, 28 development, and 31 security records; union 84 |
| Isolated build inventory | 6 hash-pinned distributions in `sbom.build.cdx.json` |
| Linux runtime inventory | 41 Python distributions, CPython 3.14.7, and 97 Debian packages; 139 components total |
| Built image | `makolet:license-audit`, image ID `sha256:827d995c06af585ed14f4aba05e68e900832ad605e3f92045c18622e4b207ea8` |
| API runtime probe | `/healthz` returned `{"status":"ok"}` using asyncio/`h11`; standard-extra modules were absent |
| Synchronization | `uv sync --locked --all-groups --dry-run` reported no changes |
| Environment consistency | `uv pip check` found the installed distributions compatible |
| Vulnerability result | No known vulnerabilities from the PyPI advisory service in the exact all-groups export or exact build closure |

Scope counts overlap. `sbom.cdx.json` is an inventory cross-check; embedded
license, notice, and SBOM files in the exact distributed artifacts remain the
stronger evidence.

### Reproducible distribution evidence

On 2026-08-16 `scripts/check_distribution.py --reproducible` completed two
independent offline, hash-constrained builds under its fixed time zone, epoch, hash
seed, and no-network inputs. Both builds produced the same 100-member wheel,
SHA-256
`926cd2e76e450d9a57272b4db62f32f5d0039edbd1ee68c5624fa46e65c65570`,
and the same 270-member sdist, SHA-256
`7bf8fbd0f9edee353655015b4801a1f878e509f687a8d0d19ede9301a97be5a8`.
Building again from the generated sdist produced a byte-identical wheel. An isolated
installation with the exactly pinned runtime closure resolved the packaged migration
head, verified license/notice and security-policy evidence, and ran the installed
`makolet --help` entry point outside the checkout. The gate removed every bounded
temporary build/install path.

Those hashes identify the exact pre-documentation-reconciliation source tree used by
that run. The documentation edits that record the evidence change the sdist input and
therefore require a new final sdist hash before release; they do not weaken the
two-build, sdist-rebuild, installation, or cleanup contract now enforced by CI.

## Exact direct pins

Every declared application, development, and security dependency is exactly
pinned.

### Runtime

| Direct pin and primary source | License/SPDX conclusion | Decision and obligation |
|---|---|---|
| [`alembic==1.19.1`](https://pypi.org/project/alembic/1.19.1/) | MIT | Compatible; retain MIT notice. |
| [`anyio==4.14.2`](https://pypi.org/project/anyio/4.14.2/) | MIT | Compatible; retain MIT notice. |
| [`asyncpg==0.31.0`](https://pypi.org/project/asyncpg/0.31.0/) | Apache-2.0 | Compatible at package level; retain license. Native wheel. |
| [`boto3==1.43.68`](https://pypi.org/project/boto3/1.43.68/) | Apache-2.0 | Compatible; preserve license and `NOTICE`. |
| [`fastapi==0.141.1`](https://pypi.org/project/fastapi/0.141.1/) | MIT | Compatible; retain MIT notice. |
| [`httpx==0.28.1`](https://pypi.org/project/httpx/0.28.1/) | BSD-3-Clause | Compatible; retain BSD notice. |
| [`prometheus-client==0.26.0`](https://pypi.org/project/prometheus-client/0.26.0/) | Apache-2.0 AND BSD-2-Clause | Compatible; preserve both licenses and `NOTICE`. |
| [`pydantic==2.13.4`](https://pypi.org/project/pydantic/2.13.4/) | MIT | Compatible; retain MIT notice. |
| [`pydantic-settings==2.15.0`](https://pypi.org/project/pydantic-settings/2.15.0/) | MIT | Compatible; retain MIT notice. |
| [`rich==15.0.0`](https://pypi.org/project/rich/15.0.0/) | MIT | Compatible; retain MIT notice. |
| [`sqlalchemy[asyncio]==2.0.51`](https://pypi.org/project/SQLAlchemy/2.0.51/) | MIT | Compatible; retain MIT notice. Native wheel. |
| [`structlog==26.1.0`](https://pypi.org/project/structlog/26.1.0/) | MIT OR Apache-2.0 | Compatible; MIT branch selected and upstream `NOTICE` preserved. |
| [`tenacity==9.1.4`](https://pypi.org/project/tenacity/9.1.4/) | Apache-2.0 | Compatible; retain license. |
| [`typer==0.27.1`](https://pypi.org/project/typer/0.27.1/) | MIT | Compatible; retain MIT notice. |
| [`tzdata==2026.3`](https://pypi.org/project/tzdata/2026.3/) | Apache-2.0 | Compatible; preserve packaged `LICENSE` and `licenses/LICENSE_APACHE`. The wheel contains `zic`-compiled IANA time-zone data. |
| [`uvicorn==0.52.1`](https://pypi.org/project/uvicorn/0.52.1/) | BSD-3-Clause | Compatible; retain BSD notice. Verified with asyncio and `h11`; the `standard` extra is not selected. |

The exact 2026.3 `tzdata`
[source license is Apache-2.0](https://github.com/python/tzdata/blob/2026.3/LICENSE).
This audit records the installed wheel's evidence without inventing a separate
license for source material the wheel does not identify.

### Development and optional benchmark support

| Direct pin and primary source | License/SPDX conclusion | Decision and obligation |
|---|---|---|
| [`mypy==2.3.0`](https://pypi.org/project/mypy/2.3.0/) | MIT AND PSF-2.0 AND Apache-2.0 | Compatible components; preserve all shipped licenses. Native/mypyc wheel. |
| [`psutil==7.2.2`](https://pypi.org/project/psutil/7.2.2/) | BSD-3-Clause | Compatible optional benchmark extra and development dependency; retain BSD notice. Native wheel. |
| [`pytest==9.1.1`](https://pypi.org/project/pytest/9.1.1/) | MIT | Compatible; retain MIT notice. |
| [`pytest-asyncio==1.4.0`](https://pypi.org/project/pytest-asyncio/1.4.0/) | Apache-2.0 | Compatible; retain license. |
| [`pytest-cov==7.1.0`](https://pypi.org/project/pytest-cov/7.1.0/) | MIT | Compatible; retain MIT notice. |
| [`respx==0.23.1`](https://pypi.org/project/respx/0.23.1/) | BSD-3-Clause | Compatible; retain BSD notice. |
| [`ruff==0.16.2`](https://pypi.org/project/ruff/0.16.2/) | MIT package; embedded CC0-1.0 and WTFPL components | External development/CI tool only. Repository-policy exception; do not redistribute in Makolet artifacts. |
| [`types-boto3[s3]==1.43.68`](https://pypi.org/project/types-boto3/1.43.68/) | MIT | Compatible; retain MIT notice. |

### Security tooling

| Direct pin and primary source | License/SPDX conclusion | Decision and obligation |
|---|---|---|
| [`pip-audit==2.10.1`](https://pypi.org/project/pip-audit/2.10.1/) | Apache-2.0 | Compatible; retain license. It does not select `pywin32`. |
| [`pip-licenses==5.5.5`](https://pypi.org/project/pip-licenses/5.5.5/) | MIT | Compatible; retain MIT notice. It does not select `pywin32`. |

The security group resolves to 31 records including shared packages. Its unique
tool closures are ordinary permissive/Apache packages plus `certifi` under
MPL-2.0; there is no `pywin32`, `adodbapi`, GPL, LGPL, or AGPL edge.

### Build system

The isolated PEP 517 environment is intentionally separate from `uv.lock`.
`build-constraints.txt` pins the wheel and sdist hashes for every selected
distribution; `sbom.build.cdx.json` records the same artifacts, hashes, licenses,
and dependency edges.

| Exact build distribution | License/SPDX conclusion |
|---|---|
| [`hatchling==1.32.0`](https://pypi.org/project/hatchling/1.32.0/) | MIT |
| [`packaging==26.3`](https://pypi.org/project/packaging/26.3/) | Apache-2.0 OR BSD-2-Clause |
| [`pathspec==1.1.1`](https://pypi.org/project/pathspec/1.1.1/) | MPL-2.0; binary/source-artifact obligations below |
| [`pluggy==1.6.0`](https://pypi.org/project/pluggy/1.6.0/) | MIT |
| [`tomlkit==0.15.1`](https://pypi.org/project/tomlkit/0.15.1/) | MIT |
| [`trove-classifiers==2026.6.1.19`](https://pypi.org/project/trove-classifiers/2026.6.1.19/) | Apache-2.0 |

`scripts/check_distribution.py --reproducible` invokes `uv build --no-sources
--offline --build-constraints build-constraints.txt --require-hashes` twice under
fixed reproducibility inputs, then repeats the wheel build from the generated sdist.
Trace-level resolution selected exactly these six versions. The gate rejects any
artifact-byte difference, validates the packaged inventory and reviewed evidence,
and installs and exercises the sdist-derived wheel without network access.

### MPL-2.0 policy decision

MPL-2.0 is OSI-approved and file-level copyleft. Makolet permits unmodified
MPL-covered dependency artifacts to be combined with its Apache-2.0 code. When
an artifact containing one is distributed, the release must prominently identify
the component and MPL-2.0, include the MPL text, and inform recipients how to
obtain the exact corresponding source; modifications to MPL-covered files must
remain available under MPL-2.0. Merely depending on or combining an unmodified
MPL artifact does not apply MPL to Makolet's Apache-2.0 files.

The exact source paths for this snapshot are:

- `certifi==2026.7.22`: [PyPI sdist](https://files.pythonhosted.org/packages/a3/c2/24167ea9858356b47a87a50d39908bfdb72ceeefe0041586e704e5376b3a/certifi-2026.7.22.tar.gz), SHA-256 `741e2c3b351ddf169a738da9f2c048608ff7f2c5cc02f1ebc6b118bb090d5d55`;
- `pathspec==1.1.1`: [PyPI sdist](https://files.pythonhosted.org/packages/5a/82/42f767fc1c1143d6fd36efb827202a2d997a375e160a71eb2888a925aac1/pathspec-1.1.1.tar.gz), SHA-256 `17db5ecd524104a120e173814c90367a96a98d07c45b2e10c2f3919fff91bf5a`.

The runtime container contains `certifi`, its installed MPL license evidence,
this notice, and the SBOMs. `pathspec` is not in the runtime image; its exact
build artifacts are identified in the build SBOM and constraints.

## Complete 84-record resolved union

Scopes are `R` runtime, `B` optional benchmark, `D` development, and `S` security. A package may occur
in more than one marker-aware export. The rows below account for every third-
party record in `uv.lock` exactly once. These are package-level classifications;
the Ruff tooling exception and base-OS platform components are tracked separately.

| Package-level license / qualifier | Count | Exact locked packages and scopes | Compatibility conclusion |
|---|---:|---|---|
| Apache-2.0 | 10 | `asyncpg==0.31.0` (R); `CacheControl==0.14.4` (S); `msgpack==1.2.1` (S); `pip-api==0.0.34` (S); `pip-audit==2.10.1` (S); `py-serializable==2.1.0` (S); `pytest-asyncio==1.4.0` (D); `sortedcontainers==2.4.0` (S); `tenacity==9.1.4` (R); `tzdata==2026.3` (R) | OSI/ASF Category A; preserve licenses and retained notices. |
| Apache-2.0 plus upstream `NOTICE` | 7 | `boto3==1.43.68` (R); `botocore==1.43.68` (R); `coverage==7.15.4` (D); `cyclonedx-python-lib==11.11.1` (S); `license-expression==30.4.4` (S); `requests==2.34.2` (S); `s3transfer==0.19.2` (R) | OSI/Category A; preserve license and applicable `NOTICE` text. |
| Apache-2.0 AND BSD-2-Clause plus `NOTICE` | 1 | `prometheus-client==0.26.0` (R) | Compatible; preserve both licenses and notice. |
| Apache-2.0 OR BSD-2-Clause | 1 | `packaging==26.3` (DS) | Compatible under either branch; preserve shipped license set. |
| BSD-3-Clause for the package; post-2017 portions additionally Apache-2.0 OR BSD-3-Clause | 1 | `python-dateutil==2.9.0.post0` (R) | Compatible; preserve the combined upstream license. |
| BSD-2-Clause | 2 | `boolean.py==5.0` (S; lock normalized as `boolean-py`); `Pygments==2.20.0` (RDS) | OSI/Category A; retain notices. |
| BSD-3-Clause | 12 | `click==8.4.2` (R); `colorama==0.4.6` (RD); `httpcore==1.0.9` (RD); `httpx==0.28.1` (RD); `idna==3.18` (RDS); `MarkupSafe==3.0.3` (R); `prettytable==3.18.0` (S); `psutil==7.2.2` (BD); `python-dotenv==1.2.2` (R); `respx==0.23.1` (D); `starlette==1.6.0` (R); `uvicorn==0.52.1` (R) | OSI/Category A; retain notices. |
| ISC | 1 | `shellingham==1.5.4` (R) | OSI/Category A; retain notice. |
| MIT | 38 | `alembic==1.19.1` (R); `annotated-doc==0.0.5` (R); `annotated-types==0.8.0` (R); `anyio==4.14.2` (RD); `botocore-stubs==1.43.67` (D); `charset-normalizer==3.4.9` (S); `fastapi==0.141.1` (R); `filelock==3.32.2` (S); `h11==0.16.0` (RD); `iniconfig==2.3.0` (D); `jmespath==1.1.0` (R); `Mako==1.4.1` (R); `markdown-it-py==4.2.0` (RS); `mdurl==0.1.2` (RS); `mypy_extensions==1.1.0` (D); `packageurl-python==0.17.6` (S); `pip-licenses==5.5.5` (S); `pip-requirements-parser==32.0.1` (S); `platformdirs==4.11.2` (S); `pluggy==1.6.0` (D); `pydantic_core==2.46.4` (R); `pydantic-settings==2.15.0` (R); `pydantic==2.13.4` (R); `pyparsing==3.3.2` (S); `pytest-cov==7.1.0` (D); `pytest==9.1.1` (D); `rich==15.0.0` (RS); `six==1.17.0` (R); `SQLAlchemy==2.0.51` (R); `tomli_w==1.2.0` (S); `tomli==2.4.1` (S); `typer==0.27.1` (R); `types-boto3-s3==1.43.66` (D); `types-boto3==1.43.68` (D); `types-s3transfer==0.16.0` (D); `typing-inspection==0.4.3` (R); `urllib3==2.7.0` (RS); `wcwidth==0.8.2` (S) | OSI/Category A; retain notices. Native artifacts require artifact-specific review before bundling. |
| MIT with embedded Rust license bundle | 1 | `ast-serialize==0.8.0` (D) | Compatible package; preserve root and `crates/LICENSE` files. |
| MIT with vendored-license bundle | 1 | `pip==26.2.1` (S) | Compatible package; preserve its entire vendored-license directory. Its vendored `certifi` CA bundle is MPL-2.0. |
| MIT AND PSF-2.0 | 2 | `greenlet==3.5.5` (R); `librt==0.15.0` (D) | Compatible; preserve both licenses. Native wheels. |
| MIT AND PSF-2.0 AND Apache-2.0 | 1 | `mypy==2.3.0` (D) | Compatible components; preserve all three license texts. Native wheel. |
| MIT OR Apache-2.0 plus `NOTICE` | 1 | `structlog==26.1.0` (R) | Compatible; MIT branch selected and upstream notice retained. |
| MIT, native executable | 1 | `ruff==0.16.2` (D) | Package metadata is MIT; embedded non-OSI components require the non-distributed-tooling exception above. |
| MPL-2.0 | 2 | `certifi==2026.7.22` (RDS); `pathspec==1.1.1` (D) | OSI, weak copyleft, ASF Category B. Binary distribution requires prominent labeling, the MPL text, and a source-availability path; do not incorporate covered source into Apache-2.0 source. |
| PSF-2.0 | 2 | `defusedxml==0.7.1` (S); `typing_extensions==4.16.0` (RD) | OSI/Category A; retain license. |

## Actual security-tool verification

`pip-audit==2.10.1` does not currently recognize `uv.lock` through
`pip-audit --locked .`; that command returned `no lockfiles found in .`.
The verified local workflow therefore exports the exact marker-aware all-groups
lock to a temporary requirements file and audits it with dependency resolution
disabled. The same no-resolution audit is run separately against the exact
hash-pinned build closure.

Both audits returned **No known vulnerabilities found** from the PyPI advisory
service. This is a point-in-time advisory result, not proof that vulnerabilities
do not exist. Auditing `site-packages` directly is not the canonical gate:
`pip-audit` attempts to resolve the unpublished editable Makolet project. The
locked export avoids that false failure without suppressing any third-party
record.

The historical `pip-licenses --format json` diagnostic returned only 81 rows,
including Makolet. It omitted its own distribution and three packages in its
execution closure: `pip`, `prettytable`, and `wcwidth`. The former checker accepted
that partial report, which is the reproduced pre-fix failure mode. The release gate
no longer treats an installed-tool report as an authoritative inventory.

`scripts/check_licenses.py` now starts from the frozen lock and build closure and
requires every identity, including `pip-licenses==5.5.5`, `pip==26.2.1`,
`prettytable==3.18.0`, and `wcwidth==0.8.2`, to have exactly one reviewed policy
classification. It also enforces the required `NOTICE`, vendored-license,
embedded-license, and packaged-license obligations; the two exact MPL source URLs
and hashes; Ruff as the sole non-redistributed tooling exception; and the exact
digest-pinned 97-package Debian inventory with copyright/license and
corresponding-source obligations. The detailed artifact analysis below remains
authoritative where it is stricter than package metadata.

## Rejected Parquet dependency candidates

### DuckDB 1.5.5: technically capable, policy hold

The current stable candidate inspected was
[`duckdb==1.5.5`](https://pypi.org/project/duckdb/1.5.5/), not the older 1.5.4
release. Its CPython 3.14 Windows AMD64 wheel:

- is `duckdb-1.5.5-cp314-cp314-win_amd64.whl`, 13,691,858 bytes, SHA-256
  `9dc826c4b50e64f6c4e4d07a3a9cb075ef70ba3899dc43ec5493dc3d7b04b353`;
- is attested to `duckdb-python` commit
  [`b236c8194ed14c7a7c685e0534dde501cc855b3a`](https://github.com/duckdb/duckdb-python/commit/b236c8194ed14c7a7c685e0534dde501cc855b3a);
- contains one `_duckdb.cp314-win_amd64.pyd`, 37,380,608 bytes, SHA-256
  `1ced4befb2e24c3e5ee9905bcdbac5d9e4be398c3e092da1f890cfd4cab582a4`,
  and no bundled DLL files; and
- has no core runtime `Requires-Dist` entries unless the optional `all` extra is
  requested. That extra was not evaluated as acceptable because it includes
  PyArrow.

DuckDB's Python CMake configuration selects the static MSVC runtime, and PE
inspection found no `VCRUNTIME` or `MSVCP` imports. The runtime object code is
therefore incorporated into the `.pyd` rather than carried as separate DLLs.
Microsoft's toolchain redistribution terms are proprietary/non-OSI, and the
wheel carries no Microsoft terms. Under the literal all-components OSI rule,
this is an independent blocker.

An isolated Python 3.14.7 probe successfully wrote Zstandard-compressed,
Hive-partitioned Parquet with fixed-precision decimals and read it back. This
matches DuckDB's official
[`COPY ... PARTITION_BY` documentation](https://duckdb.org/docs/stable/data/partitioning/partitioned_writes).

The wheel is nevertheless **not selected**. The core build at DuckDB commit
[`d8cdaa33fda8df955cc76ef58a280f68f4cd43fa`](https://github.com/duckdb/duckdb/commit/d8cdaa33fda8df955cc76ef58a280f68f4cd43fa)
statically links core functions, JSON, Parquet, and ICU. The evidenced embedded
code includes MIT, BSD-2/3-Clause, Apache-2.0, Zlib, and BSL-1.0 components;
Mbed TLS offers `Apache-2.0 OR GPL-2.0-or-later` and could be taken under Apache.
No LGPL or AGPL component was found.

In addition to the static Microsoft runtime issue, ICU brings custom
IPADIC/ICOT data wording and public-domain IANA time-zone material that does not
satisfy the literal “known OSI-approved dependencies only” rule. The wheel also
ships only the DuckDB MIT license plus an experimental Spark license: it omits
the native third-party license bundle, the required t-digest notice, and ICU
data notices. The Spark license itself refers to a missing `licenses/`
directory. Therefore neither an OSI-only nor a notice-complete conclusion is
justified for the exact wheel.

A custom no-ICU build with a complete curated license payload could be audited
later, but the PyPI wheel must not silently enter the current lock.

### Fastparquet, Rugo, and pure-Python `parquet`

- [`fastparquet==2026.5.0`](https://pypi.org/project/fastparquet/2026.5.0/)
  is Apache-2.0 and publishes a CPython 3.14 Windows wheel, but its own current
  project description says pandas 3.0 broke it and the project is being retired.
  A clean dry run resolved nine packages: Fastparquet, `cramjam`, `fsspec`,
  NumPy, `packaging`, pandas, `python-dateutil`, `six`, and `tzdata`. It is
  neither a small dependency nor a maintainable PyArrow replacement.
- [`rugo==0.4.29`](https://pypi.org/project/rugo/0.4.29/) declares Apache-2.0,
  Python `>=3.11`, and no Python runtime dependencies, but publishes no Windows
  wheel and does not expose a built-in Hive-partitioned dataset writer. It is not
  a verified Python 3.14 Windows solution.
- [`parquet==1.3.1`](https://pypi.org/project/parquet/1.3.1/) is an old
  pure-Python reader whose documented TODO is implementing writing. It cannot
  satisfy the export requirement.

No candidate was added merely to make the lock appear complete. Makolet instead
implemented an independently written, dependency-free bounded Parquet writer.
The contract suite covers deterministic partitioning, decimals, compression,
row/page bounds, nulls, and hostile values; an isolated DuckDB 1.5.4 process
successfully read the resulting dataset. That closes the engineering and format-
conformance gate without changing the dependency selection. Exact evidence is in
[`parquet-interop.md`](parquet-interop.md).

## Removed blocker evidence retained for history

### PyArrow 25.0.1 Windows wheel

The rejected artifact was
`pyarrow-25.0.1-cp314-cp314-win_amd64.whl`, lock SHA-256
`f729cfdbd36fd99d543b67a914d2de044c84ebe45be8b34902b299b608c15c8f`.
Its metadata declared Apache-2.0 and no Python `Requires-Dist`, but the wheel
contained a large native bundle, including two Microsoft Visual C++ runtime DLLs:

- `msvcp140-d448dcabdad98c14abc6ff2741df95cd.dll`, SHA-256
  `d448dcabdad98c14abc6ff2741df95cd43f7241a33ddd7d7a2857e4fd4d67777`;
- `msvcp140_atomic_wait-4bec1bd7c3f445519c6c93c879884a89.dll`, SHA-256
  `6d77283607341cb0edb7ec2ca263db84af1b9065f21785ea2525c87b0eb4199c`.

The DLLs were version 14.44.35227.0 and the wheel did not carry Microsoft's
proprietary redistribution terms. The artifact also statically embedded bzip2
1.0.8 without its custom, non-OSI notice; included CC0 material, which is not an
OSI-approved software license under the literal rule; and did not provide
component-specific evidence for several other linked libraries. Build evidence
also indicated stale OpenSSL provenance. Top-level Apache metadata could not
clear those artifact contents.

The wheel did contain Arrow `LICENSE.txt` and `NOTICE.txt`, with Apache Arrow and
numerous third-party attributions. No GPL/LGPL/AGPL component was found; the
`GPLv2` text observed in the license appendix was part of LLVM's Apache exception.
The package was rejected because of proprietary/non-OSI and notice-completeness
problems, not because Arrow itself is GPL.

### pywin32 312 Windows wheel

Top-level `pywin32` metadata stated PSF-2.0, but the installed distribution also
contained `adodbapi` under `LGPL-2.1-or-later`, Scintilla custom permissive text,
and MIT MAPI stub code. LGPL is ASF Category X for an Apache project.

The two prior paths were:

```text
pywin32==312 <- mcp==2.0.0
pywin32==312 <- docker==7.2.0 <- testcontainers==4.15.0
```

Neither direct package was imported by current source or tests. Removing those
unused pins removed `pywin32`, while retaining the actually needed
`pip-audit==2.10.1` and `pip-licenses==5.5.5` security tools.

## Native and notice-bearing current artifacts

### Windows wheels and tooling

The exact Windows environment's native payloads are in `ast-serialize`,
`asyncpg`, `charset-normalizer`, `coverage`, `greenlet`, `librt`, `MarkupSafe`,
`msgpack`, `mypy`, `psutil`, `pydantic-core`, `ruff`, `SQLAlchemy`, and `tomli`.
PE import inspection of the `.pyd` files found Python and Windows/MSVC platform
DLLs and no additional bundled DLL payload. Those host platform binaries are not
part of Makolet's source or wheel. A future Windows installer that bundles
CPython or the Microsoft runtime must inventory and comply with those exact
redistributed artifacts rather than relying on this host-only observation.

Three installed wheels provide PEP 770 embedded CycloneDX evidence:

- `ast-serialize==0.8.0`: 91 components, SBOM SHA-256
  `A00F8C49F4F22ECF3B4B8201940723C40424B61B4C7C570FC9008FB4FE5CFCD7`;
  the inspected expressions have OSI-approved selections.
- `pydantic-core==2.46.4`: 103 components, SBOM SHA-256
  `B12C8CADB9A666C8C1409F9709E7C483B9B96F4D60F4CCCC5D2EB085AF0CB1EF`.
  `r-efi==5.2.0` declares `MIT OR Apache-2.0 OR LGPL-2.1-or-later`; the MIT or
  Apache branch is selected, so no LGPL term is selected.
- `ruff==0.16.2`: 335 components, SBOM SHA-256
  `2C8B6C0753EF7F6731FAB3AD7CEBB207D5EFA653868C263E30CED4C33B039ADB`.
  Its CC0-only `notify==8.2.0` and WTFPL `terminfo==0.9.0` trigger the external-
  tooling-only exception above. `colored`, `option-ext`, and `version-ranges`
  are MPL-2.0. No other installed direct/development wheel exposes an embedded
  SBOM with a CC0-only or WTFPL component.

### Pinned Linux runtime image

The runtime image is based on the digest-pinned `python:3.14.7-slim-bookworm`
image. A generator executed inside the built image produced the committed
`sbom.runtime-linux.cdx.json` and a second run reproduced its 139-component
identity set. It covers all 41 installed third-party Python distributions,
CPython, and all 97 installed Debian packages. For every Python distribution it
records hashes for shipped license/notice/author/embedded-SBOM evidence. For
every Debian binary package it records the exact binary version, source package
and version, machine-readable license names, and
`/usr/share/doc/<package>/copyright` hash. DEP-5 `License:` declarations are
retained as Debian's exact labels instead of being guessed into SPDX expressions.
The 12 legacy package copyright files without such declarations use an explicit,
package-specific reviewed label set bound to the pinned file hash; a changed or new
legacy document fails generation pending review.

The runtime parity gate compares the complete stable CycloneDX document rather than
only component names and versions. It checks licenses, copyright/artifact hashes,
Debian source metadata, native links, component types/purls/bom-refs, generator and
base-image metadata, and every dependency edge. Array order is normalized because it
has no inventory meaning. The only ignored value is `makolet:platform`, whose kernel
release comes from the Docker host; the property itself remains mandatory and every
other metadata field is compared.

The Linux wheel set has 13 ELF extension objects across `asyncpg`, `greenlet`,
`MarkupSafe`, `pydantic-core`, and `SQLAlchemy`. `ldd` resolved every link; the
only targets are base-image libraries (`libc`, `libpthread`, and, where used,
`libstdc++`, `libgcc_s`, `libm`, `librt`, and `libdl`). No separately bundled
third-party shared library was found. Static Rust provenance for `pydantic-core`
is supplied by its embedded 103-component SBOM. The other native wheels carry
their package license evidence but no upstream static-component SBOM; the
inspection therefore does not invent static code that their artifacts do not
identify.

The container copies `THIRD_PARTY_NOTICES.md`, the main/build/runtime SBOMs, and
the exact build constraints to `/opt/makolet`. Installed wheel license evidence
and Debian copyright files remain in their original locations.

Special preservation requirements in the current selected graph include:

- complete `LICENSE*`, `COPYING*`, `NOTICE*`, `AUTHORS*`, and embedded SBOM files
  for every artifact actually redistributed;
- upstream notices for `boto3`, `botocore`, `coverage`,
  `cyclonedx-python-lib`, `license-expression`, `prometheus-client`, `requests`,
  `s3transfer`, and `structlog`;
- `pip`'s complete vendored-license directory and `ast-serialize`'s root and
  Rust-crate license files;
- prominent MPL-2.0 labeling, the MPL text, an exact-source availability path,
  and MPL licensing for covered-file modifications for `certifi` and `pathspec`;
  and
- both packaged `tzdata` license files.

The repository does not vendor the audited wheels. These requirements apply when
a release copies them into a container, wheelhouse, executable bundle, virtual
environment, installer, or similar artifact. Ruff is excluded rather than
covered by a redistribution action.

## Commands used

The following commands were run from the repository root. Temporary audit
exports were written under `%TEMP%`, not committed.

```powershell
uv --version
.venv\Scripts\python.exe -VV

uv lock --check
uv sync --locked --all-groups --dry-run
uv pip check --python .venv\Scripts\python.exe

$req = New-TemporaryFile
uv export --frozen --all-groups --no-emit-project --format requirements-txt --output-file $req
uv run pip-audit --requirement $req --no-deps --disable-pip --progress-spinner off --strict
uv run pip-audit --requirement build-constraints.txt --no-deps --disable-pip --progress-spinner off --strict
Remove-Item -LiteralPath $req

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
```

Read-only Python and PowerShell probes parsed `uv.lock`, compared its records
with the three exports and installed metadata, enumerated license/notice/native
files, inspected PyPI JSON and attestations, and ran isolated dependency dry
runs. DuckDB 1.5.5 was installed only into `%TEMP%` for its historical artifact
probe; it was not added to `.venv` or the lock. The independent Parquet
interoperability check used DuckDB 1.5.4 in its own isolated environment and did
not change the lock.

## Remaining redistribution conditions

There is no unresolved Python dependency, build-isolation, Parquet, MPL-policy,
or current Linux wheel-inventory gate. The following conditions remain attached
to the artifact that is actually released:

- Do not redistribute Ruff. If a future artifact needs to bundle it, its CC0 and
  WTFPL contents require a new explicit policy decision.
- A Windows bundle that includes CPython, MSVC runtime files, or other platform
  binaries needs a new exact-artifact SBOM and terms review; the current audit
  covers installed Python wheels, not a hypothetical installer.
- A Linux container publisher must preserve the recorded Python/Debian evidence
  and satisfy exact Debian GPL/LGPL corresponding-source and notice obligations.
  If the project instead forbids base-OS copyleft components, the current
  official Python image is an irreducible policy blocker.
- Preserve the MPL-2.0 notices, texts, exact-source paths, and covered-file rules
  for every distributed `certifi` or `pathspec` artifact.
- Re-run the lock, build closure, all three SBOM checks, vulnerability scan,
  fail-closed license-policy gate, and artifact review whenever dependencies,
  Python ABI, image digest, platform, selected groups, build backend, or package
  index change.
