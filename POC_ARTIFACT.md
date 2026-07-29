# Observable synchronous flush POC artifact

This fork publishes `org.apache.doris:flink-doris-connector-2.1:26.0.0-poc.4` to the personal `Sbaia/doris-flink-connector` GitHub Packages registry for integration testing.

The version is pinned to this provenance chain:

- Apache upstream base: `825b7bbe9647d54398eed91870219f322c617967`
- Flink 2 and JDK 21 patch: `c73add2959560899f92c2e3906d20cbc5d3282ae`
- Observable flush and visibility patches through: `e04ea44015a61e88baabc7b90645ae900cdb67ca`
- Arrow/ZSTD and deterministic serialization-drop compatibility: `453df68a4eb86ac634aecd712f2922335463937d`
- Arrow compatibility test isolation: `0999292cc6568d456f6cca51fcf17ea52b9d69e4`

The `publish-poc-package.yml` workflow runs for immutable `observable-flush-26.0.0-poc.4-build.*` tags, and can also be manually dispatched after the workflow reaches the default branch. It builds with JDK 21, runs the connector tests, publishes flattened consumer POMs with the repository-scoped `GITHUB_TOKEN`, resolves the connector from an empty Maven cache, enforces dependency convergence, and compares the downloaded JAR SHA-256 with the locally built artifact. No personal access token is stored in this repository or in the pipeline repository.

`26.0.0-poc.1` is withdrawn and must not be consumed: its package upload succeeded, but its published child POM retained a literal `${revision}` parent version and therefore could not be resolved outside the source reactor. `26.0.0-poc.3` is also withdrawn: its release test inherited an interrupt flag from a legacy cancellation test, so only the parent POM was uploaded and no consumer connector artifact exists. Both versions are left untouched as immutable failure evidence. `26.0.0-poc.2` is the first consumable observable-flush build; `26.0.0-poc.4` adds the generic Arrow/ZSTD and deterministic drop APIs required by the pipeline with an isolated compatibility fixture.

Consumers authenticate Maven server `github` using environment-backed credentials. For example, CI may map `GITHUB_ACTOR` and a read-only package token through Maven `settings.xml`; credentials must never be written to a project POM or committed settings file.

This personal package is POC-only. It is not approved for shared or production deployment, must not be treated as a release channel, and cannot be promoted in place. Production rollout requires a separately versioned, immutable package owned by Lansweeper and built from reviewed commits.

The workflow, consumer fixture, and `poc-release` Maven profile are release-only fork changes. Generic connector changes remain in earlier separate commits so they can be proposed upstream without GitHub Packages configuration.

## Published evidence

- Source commit: `af44184e4b7b2efb85c583b646d624f82088ef2d`
- Immutable build tag: `observable-flush-26.0.0-poc.2-build.1`
- GitHub Actions run: `https://github.com/Sbaia/doris-flink-connector/actions/runs/30467928849`
- Connector JAR SHA-256: `b6c36f6caac1b9fda89df297f5159f9438909897cd0c179805a48a70d3bfb907`
- Clean-cache resolution and Maven dependency convergence: passed
