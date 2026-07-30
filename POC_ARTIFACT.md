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

# Observable synchronous flush POC artifact

This fork prepares `org.apache.doris:flink-doris-connector-2.1:26.0.0-poc.11` for publication to the personal `Sbaia/doris-flink-connector` GitHub Packages registry for integration testing. The most recently published artifact remains `26.0.0-poc.10` until the immutable `poc.11` tag workflow completes.

The version is pinned to this provenance chain:

- Apache upstream base: `825b7bbe9647d54398eed91870219f322c617967`
- Flink 2 and JDK 21 patch: `c73add2959560899f92c2e3906d20cbc5d3282ae`
- Observable flush and visibility patches through: `e04ea44015a61e88baabc7b90645ae900cdb67ca`
- Arrow/ZSTD and deterministic serialization-drop compatibility: `453df68a4eb86ac634aecd712f2922335463937d`
- Arrow compatibility test isolation: `0999292cc6568d456f6cca51fcf17ea52b9d69e4`
- Exact logical-row, submitted-byte and buffered-byte accounting: `4c5c02e0`, `580bbe81`, `66665614`
- Arrow lifecycle safety and native ZSTD linkage: `a9b2d8b8`, `9eadc506`
- Flink 1/Java 8 and Flink 2 profile isolation: `f17d0fe7`, `0cf42f9e`
- Nullable nested `ROW` serialization: `bc03c63`
- Prepared `poc.11` publication source: `5346f17`

The `publish-poc-package.yml` workflow runs for immutable `observable-flush-26.0.0-poc.11-build.*` tags, and can also be manually dispatched after the workflow reaches the default branch. It builds with JDK 21, runs the connector tests, publishes flattened consumer POMs with the repository-scoped `GITHUB_TOKEN`, resolves the connector from an empty Maven cache, enforces dependency convergence, and compares the downloaded JAR SHA-256 with the locally built artifact. No personal access token is stored in this repository or in the pipeline repository.

`26.0.0-poc.1` is withdrawn and must not be consumed: its package upload succeeded, but its published child POM retained a literal `${revision}` parent version and therefore could not be resolved outside the source reactor. `26.0.0-poc.3` is also withdrawn: its release test inherited an interrupt flag from a legacy cancellation test, so only the parent POM was uploaded and no consumer connector artifact exists. Both versions are left untouched as immutable failure evidence. `26.0.0-poc.2` is the first consumable observable-flush build; later builds add serializer compatibility, exact accounting and allocator safety. `26.0.0-poc.10` is the first build that also preserves the shared Flink 1/Java 8 base while providing Arrow/ZSTD through the Flink 2 profile. `26.0.0-poc.11` additionally preserves nullable members of nested `ROW` values in both CSV and JSON serialization paths.

Consumers authenticate Maven server `github` using environment-backed credentials. For example, CI may map `GITHUB_ACTOR` and a read-only package token through Maven `settings.xml`; credentials must never be written to a project POM or committed settings file.

This personal package is POC-only. It is not approved for shared or production deployment, must not be treated as a release channel, and cannot be promoted in place. Production rollout requires a separately versioned, immutable package owned by Lansweeper and built from reviewed commits.

The workflow, consumer fixture, and `poc-release` Maven profile are release-only fork changes. Generic connector changes remain in earlier separate commits so they can be proposed upstream without GitHub Packages configuration.

## Prepared `poc.11` evidence

- Source commits: `bc03c63`, `5346f17`
- Local connector suites: 325 base tests and 13 Flink 2 tests passed
- Local Maven installation and pipeline shaded-JAR packaging: passed
- Remote tag, workflow run and immutable checksum: pending publication

## Most recent published evidence (`poc.10`)

- Source commit: `3633810b0c9455de6b15c296fb78394040c20a39`
- Immutable build tag: `observable-flush-26.0.0-poc.10-build.1`
- GitHub Actions run: `https://github.com/Sbaia/doris-flink-connector/actions/runs/30484489287`
- Connector JAR SHA-256: `c0cba961bc291bcd733ef18ebdbd2d062c288fdcf19a95f598ac4fc766819632`
- Clean-cache resolution and Maven dependency convergence: passed
