# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Apache Doris Flink Connector - enables data exchange between Apache Flink and Apache Doris OLAP database. Supports Flink versions 1.15 through 1.20.

## Build Commands

```bash
# Interactive build (prompts for Flink version)
cd flink-doris-connector && ./build.sh

# Build for specific Flink version (e.g., 1.18)
cd flink-doris-connector && mvn clean package \
  -Dflink.version=1.18.0 \
  -Dflink.major.version=1.18 \
  -Dflink.python.id=flink-python

# Note: For Flink 1.15, use -Dflink.python.id=flink-python_2.12
```

Output JAR is placed in `dist/` directory.

## Testing

```bash
# Run unit tests only
cd flink-doris-connector && mvn test

# Run a single test class
cd flink-doris-connector && mvn test -Dtest="ClassName"

# Run integration tests (requires Docker - uses testcontainers)
cd flink-doris-connector && mvn test -Dtest="*ITCase" -Dimage="apache/doris:doris-all-in-one-2.1.0"

# Run end-to-end tests
cd flink-doris-connector && mvn test -Dtest="*E2ECase" -Dimage="apache/doris:doris-all-in-one-2.1.0"
```

Integration/E2E tests use `AbstractContainerTestBase` and `AbstractITCaseService` base classes with Doris testcontainers.

## Code Quality

```bash
# Format code (AOSP style via google-java-format)
cd flink-doris-connector && mvn spotless:apply

# Check formatting
cd flink-doris-connector && mvn spotless:check

# Validate checkstyle (checkstyle 8.14 config in tools/maven/checkstyle.xml)
cd flink-doris-connector && mvn clean compile checkstyle:checkstyle
```

## Architecture

Single-module Maven project under `flink-doris-connector/`. Main source is in `org.apache.doris.flink` package.

### Key Packages

- **`sink/`** - Doris sink implementation using Stream Load API
  - `DorisSink` - Main Flink sink connector
  - `writer/` - Write logic, buffering, metrics
  - `committer/` - Two-phase commit support
  - `batch/` - Batch write mode
  - `copy/` - Copy-into write mode
  - `schema/` - Schema change management

- **`source/`** - Doris source connector
  - `DorisSource` - Main Flink source connector

- **`table/`** - Flink SQL/Table API integration
  - `DorisDynamicTableFactory` - Factory for SQL connector registration
  - `DorisConfigOptions` - All SQL configuration options
  - `DorisDynamicTableSink/Source` - Table API implementations
  - Lookup join support via `DorisRowDataJdbcLookupFunction`

- **`catalog/`** - Flink Catalog integration for Doris metadata

- **`cfg/`** - Configuration classes (`DorisOptions`, `DorisExecutionOptions`, `DorisReadOptions`)

- **`rest/`** - REST API client for Doris FE communication

- **`backend/`** - Backend node communication

- **`lookup/`** - Lookup table functionality

### Data Flow

1. **Sink path**: Records -> DorisWriter -> DorisStreamLoad (HTTP Stream Load) -> Doris BE
2. **Source path**: Doris tablet splits -> DorisSource reader -> Flink records
3. **SQL**: DorisDynamicTableFactory creates sink/source based on DDL options

### CDC Support

Supports Flink CDC connectors (MySQL, Oracle, PostgreSQL, SQLServer, DB2, MongoDB) for real-time synchronization to Doris. Related serializers in `sink/writer/serializer/jsondebezium/`.

## Code Style Notes

- Uses AOSP (Android Open Source Project) Java formatting style
- Import order: org.apache.flink, org.apache.flink.shaded, (blank), javax, java, scala, static imports
- Checkstyle rules prohibit: star imports, trailing whitespace, TODO with usernames, certain shaded imports
- Use Flink's Preconditions instead of Guava's
- Java 8 source/target compatibility
