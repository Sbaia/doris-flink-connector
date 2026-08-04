// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.flink.sink.batch;

import org.apache.doris.flink.exception.DorisBatchLoadException;
import org.apache.doris.flink.rest.models.RespContent;

import java.io.IOException;
import java.io.Serializable;
import java.util.Locale;

import static org.apache.doris.flink.cfg.DorisExecutionOptions.DEFAULT_LOAD_VISIBILITY_POLL_INTERVAL_MS;
import static org.apache.doris.flink.cfg.DorisExecutionOptions.DEFAULT_LOAD_VISIBILITY_TIMEOUT_MS;
import static org.apache.doris.flink.sink.LoadStatus.PUBLISH_TIMEOUT;
import static org.apache.doris.flink.sink.LoadStatus.SUCCESS;
import static org.apache.doris.flink.sink.writer.LoadConstants.GROUP_COMMIT_ASYNC_MODE;

/** Validates that a physical Stream Load is completely visible before exposing it to a caller. */
final class BatchLoadOutcomeValidator implements Serializable {
    private static final long serialVersionUID = 1L;
    private final LoadStateProvider loadStateProvider;
    private final Sleeper sleeper;
    private final TimeSource timeSource;
    private final long pollIntervalMillis;
    private final long timeoutMillis;

    BatchLoadOutcomeValidator(LoadStateProvider loadStateProvider) {
        this(
                loadStateProvider,
                Thread::sleep,
                System::currentTimeMillis,
                DEFAULT_LOAD_VISIBILITY_POLL_INTERVAL_MS,
                DEFAULT_LOAD_VISIBILITY_TIMEOUT_MS);
    }

    BatchLoadOutcomeValidator(
            LoadStateProvider loadStateProvider,
            Sleeper sleeper,
            TimeSource timeSource,
            long pollIntervalMillis,
            long timeoutMillis) {
        if (loadStateProvider == null || sleeper == null || timeSource == null) {
            throw new IllegalArgumentException("Visibility collaborators must not be null");
        }
        if (pollIntervalMillis <= 0 || timeoutMillis <= 0) {
            throw new IllegalArgumentException("Visibility polling bounds must be positive");
        }
        this.loadStateProvider = loadStateProvider;
        this.sleeper = sleeper;
        this.timeSource = timeSource;
        this.pollIntervalMillis = pollIntervalMillis;
        this.timeoutMillis = timeoutMillis;
    }

    BatchLoadResult validate(
            long firstSequence,
            long lastSequence,
            String database,
            String table,
            long submittedRows,
            long submittedBytes,
            RespContent response)
            throws InterruptedException {
        validateExactCounts(submittedRows, response);
        String terminalState = requireVisible(database, response);
        return BatchLoadResult.completed(
                firstSequence,
                lastSequence,
                database,
                table,
                submittedRows,
                submittedBytes,
                terminalState,
                response);
    }

    static void validateVisibleAckGroupCommitMode(String groupCommitMode) {
        if (groupCommitMode != null
                && GROUP_COMMIT_ASYNC_MODE.equalsIgnoreCase(groupCommitMode.trim())) {
            throw new DorisBatchLoadException(
                    "group_commit=async_mode acknowledges WAL acceptance before visibility and "
                            + "cannot be used with visible Stream Load results");
        }
    }

    private void validateExactCounts(long submittedRows, RespContent response) {
        if (response == null) {
            throw invalid("missing Stream Load response", null);
        }
        if (!SUCCESS.equals(response.getStatus())
                && !PUBLISH_TIMEOUT.equals(response.getStatus())) {
            throw invalid("non-success Stream Load status", response);
        }
        if (response.getNumberTotalRows() == null
                || response.getNumberLoadedRows() == null
                || response.getNumberFilteredRows() == null
                || response.getNumberUnselectedRows() == null) {
            throw invalid("missing row outcome counts", response);
        }
        if (response.getNumberTotalRows() < 0
                || response.getNumberLoadedRows() < 0
                || response.getNumberFilteredRows() < 0
                || response.getNumberUnselectedRows() < 0) {
            throw invalid("negative row outcome counts", response);
        }
        if (response.getNumberTotalRows() != submittedRows) {
            throw invalid(
                    "total rows "
                            + response.getNumberTotalRows()
                            + " do not match submitted rows "
                            + submittedRows,
                    response);
        }
        if (response.getNumberLoadedRows() != submittedRows) {
            throw invalid(
                    "loaded rows "
                            + response.getNumberLoadedRows()
                            + " do not match submitted rows "
                            + submittedRows,
                    response);
        }
        if (response.getNumberFilteredRows() != 0) {
            throw invalid("filtered rows must be zero", response);
        }
        if (response.getNumberUnselectedRows() != 0) {
            throw invalid("unselected rows must be zero", response);
        }
    }

    private String requireVisible(String database, RespContent response)
            throws InterruptedException {
        if (SUCCESS.equals(response.getStatus())) {
            return "VISIBLE";
        }
        String label = response.getLabel();
        if (label == null || label.trim().isEmpty()) {
            throw invalid("Publish Timeout response is missing its label", response);
        }

        long deadline = timeSource.currentTimeMillis() + timeoutMillis;
        String lastState = null;
        while (timeSource.currentTimeMillis() <= deadline) {
            try {
                lastState = normalizeState(loadStateProvider.getLoadState(database, label));
            } catch (IOException | RuntimeException e) {
                throw new DorisBatchLoadException(
                        diagnosticPrefix(response)
                                + ": transaction state query failed: "
                                + LoadDiagnosticSanitizer.text(e.getMessage()),
                        e);
            }
            if ("VISIBLE".equals(lastState)) {
                return lastState;
            }
            if ("ABORTED".equals(lastState) || "UNKNOWN".equals(lastState)) {
                throw invalid("transaction state is " + lastState, response);
            }
            if (!"PREPARE".equals(lastState) && !"COMMITTED".equals(lastState)) {
                throw invalid("malformed transaction state " + lastState, response);
            }
            long remaining = deadline - timeSource.currentTimeMillis();
            if (remaining <= 0) {
                break;
            }
            sleeper.sleep(Math.min(pollIntervalMillis, remaining));
        }
        throw invalid(
                "visibility timeout after "
                        + timeoutMillis
                        + " ms; last transaction state was "
                        + lastState,
                response);
    }

    private String normalizeState(String state) {
        if (state == null || state.trim().isEmpty()) {
            return "<missing>";
        }
        return state.trim().toUpperCase(Locale.ROOT);
    }

    private DorisBatchLoadException invalid(String reason, RespContent response) {
        return new DorisBatchLoadException(diagnosticPrefix(response) + ": " + reason);
    }

    private String diagnosticPrefix(RespContent response) {
        if (response == null) {
            return "Stream Load validation failed";
        }
        return "Stream Load validation failed for label="
                + LoadDiagnosticSanitizer.text(response.getLabel())
                + ", txnId="
                + response.getTxnId()
                + ", status="
                + LoadDiagnosticSanitizer.text(response.getStatus());
    }

    @FunctionalInterface
    interface LoadStateProvider extends Serializable {
        String getLoadState(String database, String label) throws IOException;
    }

    @FunctionalInterface
    interface Sleeper extends Serializable {
        void sleep(long millis) throws InterruptedException;
    }

    @FunctionalInterface
    interface TimeSource extends Serializable {
        long currentTimeMillis();
    }
}
