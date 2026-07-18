# Modernization Plan — skus

Audit date: 2026-07-18. Branch: `claude/repo-modernization-audit-6se2sn`.

## Repo profile

- **What**: SKU similarity/recommendation engine on Apache Spark (RDD +
  DataFrame JSON ingestion). ~210 LOC of Scala across 4 main files + 1 spec.
- **Language / build**: Scala 2.11.8, sbt 0.13.16, ScalaTest `WordSpec` suite.
- **CI**: `.github/workflows/ci.yml` — checkout@v4, setup-java@v4
  (Temurin **8**), sbt/setup-sbt@v1, `sbt test`.
- No CLAUDE.md / AGENTS.md in this repo.

## Baseline build status (before)

| Check | Result |
| --- | --- |
| `sbt test` | **Won't run.** sbt 0.13.16 launcher cannot be resolved (0.13 artifacts live on retired repos; not on Maven Central). Even if bootstrapped, Scala 2.11.8 / Spark 2.3.0 do not run on Java 21 (the only JDK here); they require JDK 8. |
| Lint/format | scalariform sbt plugin pinned (`sbt-scalariform` 1.6.0, sbt 0.13-only, project unmaintained). Not runnable. |

## Dependency inventory (current → latest stable, Maven Central, 2026-07)

| Dependency | Current | Latest stable | Notes |
| --- | --- | --- | --- |
| Scala | 2.11.8 (2016) | 2.13.18 | 2.11 EOL since 2017; Spark 4 requires 2.13. |
| Apache Spark (core/sql/hive) | 2.3.0 (2018) | 4.2.0 (released 2026-07-11); 4.1.3 mature patch line | Spark 2.3 is EOL, JDK 8-only, and carries many known CVEs (e.g. CVE-2018-17190 no-auth RCE in standalone master, CVE-2019-10099 unencrypted local spill, CVE-2020-9480 standalone auth bypass RCE, plus vulnerable transitive Jackson/log4j 1.x/Guava/Netty). Target **4.1.3** (conservative; 4.2.0 is one week old — see Deferred). |
| spark-hive | 2.3.0 | — | **Unused** — no Hive/`enableHiveSupport` usage anywhere. Remove instead of upgrading (drops a large vulnerable transitive tree). |
| ScalaTest | 3.0.1 | 3.2.20 | 3.0 `WordSpec`/`Matchers` moved to `wordspec.AnyWordSpec` / `matchers.should.Matchers` in 3.1+. |
| sbt | 0.13.16 | 1.11.7 | 0.13 launcher no longer resolvable here. |
| sbt-scalariform | 1.6.0 | (project dead) | Replace with **sbt-scalafmt 2.5.5 + scalafmt 3.9.x** (pin below latest 3.11 for sbt-plugin compat, see plan). |
| sbt-revolver | 0.9.1 | 0.10.0 | Used for `re-start` of a server app; this repo has no `main`. Remove. |

## Code findings (read-through)

1. **Bug — breaker coefficient overflow** (`SuggestionsEngine.compare`):
   `Math.pow(100, n).toInt` overflows for n ≥ 5 (100^5 > Int.MaxValue), so the
   initial coefficient saturates at `Int.MaxValue` and after a few `/= 100`
   steps the trailing attributes get weight **0** — the positional tie-breaker
   silently ignores differences in later attributes for real datasets (the
   bundled fixture has 10 attributes). Fix: `BigInt(100).pow(n)`. Add a
   regression test with ≥ 5 attributes.
2. **Fragile — positional column access** (`parseSku`): `row(0)`/`row(1)`
   relies on Spark's alphabetical JSON column ordering (`attributes`, `sku`).
   Replace with name-based `getAs[String]("sku"/"attributes")`.
3. Test suite has an unused `ScalaFutures` mixin and a `printInt` dead local —
   clean up while migrating to ScalaTest 3.2.
4. Spark 4 on JDK 21 needs `Test / fork := true` plus the standard
   `--add-opens` JVM module options for in-JVM (sbt test) execution.

## Prioritized checklist

- [x] P0 Commit this audit plan.
- [x] P1 Toolchain: sbt → 1.11.7 (`project/build.properties`), Scala → 2.13.18,
      Spark → 4.1.3, ScalaTest → 3.2.20; drop spark-hive; migrate test to
      `AnyWordSpec`; add fork + `--add-opens` for tests; verify `sbt test`
      green locally on Java 21.
- [x] P2 Fix breaker-overflow bug + regression test.
- [x] P3 `parseSku` by column name (+ keep behavior identical on fixture).
- [x] P4 Replace scalariform/revolver plugins with scalafmt; add
      `.scalafmt.conf`; format; wire `scalafmtCheckAll` into CI.
- [x] P5 CI: JDK 8 → Temurin 21, keep checkout@v4/setup-java@v4/setup-sbt@v1,
      sbt cache; mirror locally-verified commands.
- [x] P6 README updated to reality (`.gitignore` was already accurate); final
      "Done vs Deferred" below.

## Done vs Deferred

### Done

- **Toolchain**: sbt 0.13.16 → **1.11.7**, Scala 2.11.8 → **2.13.18**,
  Spark 2.3.0 → **4.1.3**, ScalaTest 3.0.1 → **3.2.20**, all verified on
  Temurin **21** (the CI JDK). Test JVM is forked with the standard
  `--add-opens` set Spark needs on JDK 17+.
- **Dependencies removed**: `spark-hive` (unused), sbt plugins
  `sbt-scalariform` 1.6.0 (project dead, sbt 0.13-only) and `sbt-revolver`
  0.9.1 (no runnable app to `re-start`).
- **Bug fixed** (with regression test): `Math.pow(100, n).toInt` overflow in
  `SuggestionsEngine.compare` zeroed tie-breaker weights for every attribute
  after the fifth; now `BigInt(100).pow(n)`.
- **Robustness**: `parseSku` reads DataFrame columns by name instead of
  relying on alphabetical JSON column ordering.
- **Tests strengthened**: the end-to-end suggestions test previously asserted
  nothing (log-only); it now checks cardinality, top-N size, and that the
  pivot SKU is its own zero-difference best match. Suite: 2 → 3 tests.
- **Formatting**: scalafmt 3.9.8 via sbt-scalafmt 2.5.6, config in
  `.scalafmt.conf` (mirrors the old scalariform style where cheap, e.g.
  spaces in import braces); `scalafmtCheckAll scalafmtSbtCheck` enforced
  in CI.
- **CI**: JDK 8 → Temurin 21 on `ubuntu-latest`; kept `actions/checkout@v4`,
  `actions/setup-java@v4` (+ sbt cache), `sbt/setup-sbt@v1`; added the
  format check. CI steps are exactly the commands verified locally.
- **Security**: leaving the Spark 2.3/Scala 2.11 line resolves the known
  Spark 2.x advisories (CVE-2018-17190, CVE-2019-10099, CVE-2020-9480, …)
  and sheds the EOL transitive stack (log4j 1.x, old Jackson/Guava/Netty,
  and the entire Hive 1.2 tree that spark-hive dragged in).

### Deferred (deliberately untouched)

- **Spark 4.2.0**: stable but released 2026-07-11 (one week before this
  work). The bump is a one-line `sparkVersion` change once it has soaked.
- **Scala 3 migration**: Spark's Scala artifacts remain 2.13-only; nothing
  to do until upstream cross-publishes.
- **Public API and algorithm semantics**: `SuggestionsEngineT`, `Sku`,
  `SkuDifferences`, comparison/breaker semantics (beyond the overflow fix),
  and the `att-<letter>-<int>` input format are unchanged on purpose.
- **`convertToArray` leniency**: malformed attribute tokens are silently
  skipped (left at 0). That is current documented-by-test behavior; making
  it strict would change results on dirty data, so it stays.
- The `master` branch name, MIT license, and repository layout.

## Final summary (PR-description style)

Modernizes the repo from a 2018-era stack (Scala 2.11 / Spark 2.3 /
sbt 0.13 / JDK 8) that can no longer even bootstrap (sbt 0.13 artifacts are
unresolvable; JDK 8 unavailable here) to the current LTS stack, in five
commits:

1. `docs:` this audit plan.
2. `build:` toolchain migration (sbt 1.11.7, Scala 2.13.18, Spark 4.1.3,
   ScalaTest 3.2.20, JDK 21 test-JVM flags, dead plugins replaced by
   scalafmt, spark-hive dropped, spec migrated to ScalaTest 3.2 APIs).
3. `fix:` breaker-coefficient Int overflow (+ regression test) and
   name-based DataFrame column access in `parseSku`.
4. `style:` scalafmt applied to all Scala sources.
5. `ci:`/`docs:` workflow on Temurin 21 with format gate; README matches
   reality.

**Risk areas**: Spark 2.3 → 4.1 is a two-major jump; the engine only uses
`SparkSession.read.json`, `DataFrame.rdd`, and core RDD ops (`map`,
`sortBy`, `take`), all stable across the jump, and the full pipeline runs
green against the bundled 20-SKU fixture. The breaker fix intentionally
changes recommendation *ordering* for datasets with ≥ 5 attributes where
ties were previously broken wrongly (trailing attributes ignored) — that is
the bug being fixed. Local verification: `sbt scalafmtCheckAll
scalafmtSbtCheck test` → 3/3 tests pass on Temurin 21.0.10 / sbt 1.11.7.
