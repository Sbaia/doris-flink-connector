# Flink 2.0 Migration Guide

## Overview

This document describes the migration of the Apache Doris Flink Connector from Flink 1.x to Flink 2.0.0.

**Important:** Apache Flink 2.0 is a major release with significant breaking changes to its APIs. Following the same pattern, this connector release drops Flink 1.x support entirely and targets Flink 2.0+ exclusively.

## Breaking Changes

### 1. Dropped Flink 1.x Support

- **Minimum Flink version:** 2.0.0
- **Minimum Java version:** 11 (required by Flink 2.0)
- Previous Flink versions (1.15, 1.16, 1.17, 1.18, 1.19, 1.20) are no longer supported

### 2. Removed Components

#### DorisSourceFunction (Removed)
The legacy `DorisSourceFunction` class has been removed. It was based on the deprecated `SourceFunction` API which was removed in Flink 2.0.

**Migration:** Use `DorisSource` (Source V2 API) instead:
```java
// Old (removed)
// DorisSourceFunction sourceFunction = ...

// New
DorisSource<RowData> source = DorisSource.<RowData>builder()
    .setDorisOptions(options)
    .setDorisReadOptions(readOptions)
    .setDeserializer(new SimpleListDeserializationSchema())
    .build();

env.fromSource(source, WatermarkStrategy.noWatermarks(), "Doris Source");
```

#### CDC Sync Tools (Removed)
All CDC (Change Data Capture) synchronization tools have been removed:
- `DatabaseSync` and all database-specific sync classes
- MySQL, Oracle, PostgreSQL, SQLServer, DB2, MongoDB sync implementations
- `CdcTools` entry point

**Reason:** Flink CDC does not yet support Flink 2.0. Once Flink CDC releases a Flink 2.0-compatible version, CDC sync functionality may be re-added.

**Workaround:** Continue using the Flink 1.x connector version for CDC sync use cases until Flink CDC supports Flink 2.0.

### 3. API Changes

#### Sink API (FLIP-372)
The connector now uses the new Sink V2 API:
- `Sink.InitContext` → `WriterInitContext`
- Writers now implement `StatefulSinkWriter<IN, DorisWriterState>`

#### Table API (FLIP-164)
- `TableSchema` → `ResolvedSchema`
- Field names and types are now extracted from `ResolvedSchema.getColumnNames()` and `ResolvedSchema.getColumnDataTypes()`

#### Lookup Functions
- `TableFunction<RowData>` → `LookupFunction`
- `AsyncTableFunction<RowData>` → `AsyncLookupFunction`

## Rationale

### Why Drop Flink 1.x Support?

1. **API Incompatibility:** Flink 2.0 removes many deprecated APIs that cannot be bridged with compatibility layers
2. **Maintenance Burden:** Supporting both Flink 1.x and 2.x would require maintaining two separate codebases
3. **Community Direction:** Following Flink's own approach of treating 2.0 as a clean break

### Why Remove CDC Sync?

The CDC sync functionality depends on Flink CDC connectors (debezium-based), which:
- Have not yet released a Flink 2.0-compatible version
- Use APIs that were removed in Flink 2.0

Once Flink CDC releases Flink 2.0 support, we plan to re-add this functionality.

## Build & Test

### Building
```bash
cd flink-doris-connector
mvn clean package -Dflink.version=2.0.0 -Dflink.major.version=2.0
```

### Running Tests
```bash
# Unit tests (requires Java 17+ with Arrow JVM flags)
JAVA_HOME=/path/to/java17 \
MAVEN_OPTS="--add-opens=java.base/java.nio=ALL-UNNAMED" \
mvn test -Dflink.version=2.0.0 -Dflink.major.version=2.0 \
    -DforkCount=0 '-Dtest=!*ITCase,!*E2ECase'

# Integration tests (requires Docker)
mvn test -Dtest="*ITCase" -Dimage="apache/doris:doris-all-in-one-2.1.0"
```

## Version Compatibility Matrix

| Connector Version | Flink Version | Java Version | CDC Support |
|-------------------|---------------|--------------|-------------|
| 25.1.0+           | 2.0.x         | 11+          | No (pending)|
| 24.x.x            | 1.15 - 1.20   | 8+           | Yes         |

## Migration Checklist

- [ ] Update Flink dependency to 2.0.0
- [ ] Update Java version to 11+
- [ ] Replace `DorisSourceFunction` with `DorisSource`
- [ ] Update any custom serializers to new API signatures
- [ ] Remove CDC sync job configurations (temporarily unsupported)
- [ ] Test with `--add-opens=java.base/java.nio=ALL-UNNAMED` JVM flag for Arrow

## Known Issues

1. **JVM Forking in Tests:** Some test environments may experience JVM crashes when forking. Use `-DforkCount=0` as a workaround.

2. **Arrow Memory Access:** Java 9+ requires `--add-opens=java.base/java.nio=ALL-UNNAMED` for Arrow operations.

## References

- [FLIP-372: Sink API Evolution](https://cwiki.apache.org/confluence/display/FLINK/FLIP-372%3A+Allow+TwoPhaseCommittingSink+WithPreCommitTopology+to+alter+the+type+of+Committable)
- [FLIP-164: Unified Flink Sink API](https://cwiki.apache.org/confluence/display/FLINK/FLIP-164%3A+Unified+Sink+API)
- [Flink 2.0 Release Notes](https://nightlies.apache.org/flink/flink-docs-release-2.0/)
