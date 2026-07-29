<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Observable batch flush upstream contribution plan

## Purpose and baseline

This document turns the downstream proof branch into a reviewable Apache Doris connector patch
stack. The downstream branch is an integration and artifact-publication branch; it must not be
opened as one monolithic upstream pull request.

- Upstream baseline: `apache/doris-flink-connector@825b7bbe9647d54398eed91870219f322c617967`
- Downstream proof head: `Sbaia/doris-flink-connector@17c86dd4b2ab0b4a3091a0850d8c9123f2fa38c9`
- Existing Flink 2/JDK 21 prerequisite: `c73add2959560899f92c2e3906d20cbc5d3282ae`
- Proven personal artifact: `26.0.0-poc.9`, development-only
- Artifact SHA-256: `69351a40182af43bac735981ccde332a1ef5b3aad2bbe82740b71e26be531cc2`

Retarget each contribution onto the current upstream head immediately before submission. Keep the
JDK 21 prerequisite separate: if it has not landed, base the stack on that open contribution rather
than copying its workflow and build changes into the observable-flush pull requests.

## Patch stack

Each patch below is an independently reviewable commit or pull request. Later patches are stacked
only where their API dependency requires it. Construct them with interactive staging from the
listed provenance commits; do not cherry-pick the proof branch wholesale because its CI publication
commits are intentionally interleaved with generic development.

### 1. Add immutable batch flush result values

**Intent:** Introduce payload-free value objects for one load and one caller drain, including exact
logical rows and physical submitted bytes. This patch contains no waiting, HTTP or validation policy.

- New API: `BatchLoadResult`, `BatchFlushResult`.
- Compatibility: additive API only; existing `checkpointFlush()` and sink behavior remain unchanged.
- Source provenance: `fe368a0f`, `4c5c02e0`, `580bbe81`, and the result-value hunks of `66665614`.
- Focused tests: `BatchFlushResultTest`; value equality, immutability, overflow-safe aggregation,
  empty results, logical-row count and submitted-byte aggregation.

### 2. Track bounded, non-overlapping drain epochs

**Intent:** Sequence every enqueued Stream Load, wait through a captured watermark, and return each
completed load exactly once. Auto-flushed loads before the watermark belong to the drain; later loads
do not. Completed results are discarded after a successful drain so tracking remains bounded.

- Main implementation: `BatchRecordBuffer`, `DorisBatchStreamLoad`, and the additive writer entry
  point that returns the result.
- Compatibility: keep the existing void flush methods as delegating wrappers; callers that do not use
  the new API retain their current semantics.
- Source provenance: the tracker and writer hunks of `fe368a0f`, all of `c1f16e8d`, logical-row
  propagation from `4c5c02e0`, and exact byte propagation from `66665614`.
- Focused tests: `DorisBatchStreamLoadFlushResultTest` and `TestDorisBatchWriter`; empty,
  consecutive, multi-buffer, auto-flush, later-load exclusion, failure, retry and bounded-retention
  cases.

### 3. Add bounded transaction-visibility lookup

**Intent:** Resolve an accepted Stream Load transaction to terminal Doris visibility. A
`Publish Timeout` response is not treated as visible; its numeric transaction ID is polled with a
configured interval and deadline.

- Main implementation: fixed-shape transaction status lookup in `RestService`, visibility polling
  options in `DorisExecutionOptions`, and sanitized diagnostics in `LoadDiagnosticSanitizer`.
- Compatibility: polling defaults apply only when the observable API is used; existing sink calls do
  not acquire a new synchronous wait.
- Security: validate numeric transaction IDs, keep the query shape fixed, and never expose password,
  authenticated URL, payload sample or full Doris response through the result.
- Source provenance: visibility and diagnostic-helper hunks of `e04ea440`.
- Focused tests: `TestRestService`, `DorisExecutionOptionsTest`, and visibility cases in
  `DorisBatchStreamLoadFlushResultTest`; immediate visible, publish timeout then visible, aborted,
  malformed ID and timeout.

### 4. Enforce strict visible and exact outcomes

**Intent:** Provide an explicit validator for callers that require a durable visible outcome. Success
requires every load to be `VISIBLE`, submitted/total/loaded rows to agree, and filtered and unselected
rows to be zero. WAL acceptance, `COMMITTED`, HTTP 2xx and `Publish Timeout` alone fail closed.

- Main implementation: `BatchLoadOutcomeValidator` and its integration at the observable drain
  boundary; explicitly reject asynchronous group commit for strict mode.
- Compatibility: strict validation is opt-in through the new observable API; the existing official
  terminal sink keeps its published at-least-once contract.
- Source provenance: validator and integration hunks of `e04ea440`.
- Focused tests: `BatchLoadOutcomeValidatorTest`, `BatchFlushResultTest`, and strict cases in
  `DorisBatchStreamLoadFlushResultTest`; mismatched totals, filtered/unselected rows, committed-only,
  aborted, missing status and sanitized failure.

### 5. Preserve Arrow format compatibility and lifecycle safety

This is a separate supporting contribution, not part of the four core observable-flush patches.
It restores the Arrow serializer on the Flink 2 artifact, makes dropped batches generically
observable, closes allocator-backed writers, prevents HTTP gzip properties leaking into Arrow
transport, and preserves the original `zstd-jni` package required by native symbol lookup.

- Source provenance: `453df68a`, `a9b2d8b`, and only the generic source/POM hunks of `9eadc506`.
- Focused tests: `RowDataSerializerCompatibilityTest`, including CSV, JSON, Arrow, Arrow+ZSTD,
  failure recovery, listener isolation and allocator release.
- Packaging test: run a clean external consumer against the packaged Flink 2 JAR and execute a real
  ZSTD round trip. The `.github/poc-consumer` fixture may be adapted for upstream CI, but personal
  package coordinates and publication workflow must not be copied.

## Downstream-only changes

The following remain in the personal fork or move to an organization fork. They are not part of an
Apache contribution:

- `.github/workflows/publish-poc-package.yml`, `POC_ARTIFACT.md`, personal package coordinates,
  immutable POC tags, GitHub Packages permissions and clean-cache publication evidence;
- commits `01abd5f1`, `84088356`, `53fd013a`, `af44184e`, `109ba989`, `4be4ccf4`, `60cd6635`,
  `1ded2af0`, `ce87673d`, `ea165bf3`, `06744a64`, `e32bdee5`, `53c28c77`, `b4a9718a`, and
  `17c86dd4`;
- `.github/poc-consumer` changes in `9eadc506`; stage its generic ZSTD source/POM changes separately;
- pipeline ACK, manifest, routing, dead-letter, barrier, canary and rollback behavior. Those concepts
  belong to the downstream application, not to the connector API.

`0999292c` is a generic Arrow test-isolation change and may be folded into patch 5 after retargeting.
The existing Actions changes for Flink 2/JDK 21 remain with the separate prerequisite contribution.

## Review and compatibility checklist

Before opening each pull request:

1. Rebase its topic branch onto current `upstream/master` and record the new base plus the source
   provenance above in the pull-request description.
2. Verify every added Java file has the ASF header. Leave `LICENSE.txt` and `NOTICE.txt` unchanged
   unless the patch actually adds a distributable third-party work that requires an update.
3. Run the base-module unit suite with Flink 2.1 and the repository's supported JDK:

   ```bash
   ./mvnw -f flink-doris-connector/pom.xml -Pflink2 \
     -pl flink-doris-connector-base \
     -Dflink.version=2.1.2 -Dflink.major.version=2.1 test
   ```

4. Run the focused tests listed for the patch, Maven dependency convergence, formatting/license
   checks exposed by the current upstream build, and the existing Flink 1 compatibility suite.
5. For patches 2-4, run the live Doris matrix for immediate success, `Publish Timeout`, rejection,
   remote failure and consecutive epochs. For patch 5, run every supported format from a packaged
   clean-cache consumer with Arrow allocator debugging enabled.
6. Search only the proposed diff for downstream vocabulary. Apache changes must contain no
   Lansweeper ACK, manifest, routing, dead-letter, batch-ID or barrier contract.
7. Include public-API compatibility notes. The stack is additive: existing terminal sink users do
   not opt into strict visibility, do not receive new output, and retain current checkpoint behavior.

## Downstream release gate

The pipeline must pin an immutable organization-owned connector artifact until an official Apache
release contains equivalent tested APIs. An upstream merge or snapshot is insufficient. Promotion
requires a released version, clean-cache resolution, checksum/provenance verification, the connector
unit and packaged-runtime suites, the pipeline full reactor, and the live Doris writer-to-canonical
ACK suite. Only then may the organization pin be replaced; rollback remains a version pin change.
