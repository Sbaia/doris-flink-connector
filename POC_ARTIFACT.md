# Observable synchronous flush POC artifact

This fork publishes `org.apache.doris:flink-doris-connector-2.1:26.0.0-poc.1` to the personal `Sbaia/doris-flink-connector` GitHub Packages registry for integration testing.

The version is pinned to this provenance chain:

- Apache upstream base: `825b7bbe9647d54398eed91870219f322c617967`
- Flink 2 and JDK 21 patch: `c73add2959560899f92c2e3906d20cbc5d3282ae`
- Observable flush and visibility patches through: `e04ea44015a61e88baabc7b90645ae900cdb67ca`

The `publish-poc-package.yml` workflow runs for the immutable `observable-flush-26.0.0-poc.1` tag, and can also be manually dispatched after the workflow reaches the default branch. It builds with JDK 21, runs the connector tests, publishes the reactor with the repository-scoped `GITHUB_TOKEN`, resolves it from an empty Maven cache, enforces dependency convergence, and compares the downloaded JAR SHA-256 with the locally built artifact. No personal access token is stored in this repository or in the pipeline repository.

Consumers authenticate Maven server `github` using environment-backed credentials. For example, CI may map `GITHUB_ACTOR` and a read-only package token through Maven `settings.xml`; credentials must never be written to a project POM or committed settings file.

This personal package is POC-only. It is not approved for shared or production deployment, must not be treated as a release channel, and cannot be promoted in place. Production rollout requires a separately versioned, immutable package owned by Lansweeper and built from reviewed commits.

The workflow and consumer fixture are release-only fork files. Generic connector changes remain in separate commits so they can be proposed upstream without GitHub Packages configuration.
