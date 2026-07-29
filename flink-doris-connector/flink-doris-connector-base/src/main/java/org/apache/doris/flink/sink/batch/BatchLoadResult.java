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

import org.apache.doris.flink.rest.models.RespContent;

import java.io.Serializable;

/** Immutable outcome of one physical Doris Stream Load request. */
public final class BatchLoadResult implements Serializable {
    private static final long serialVersionUID = 1L;

    private final long firstSequence;
    private final long lastSequence;
    private final String database;
    private final String table;
    private final String label;
    private final Long transactionId;
    private final String status;
    private final String existingJobStatus;
    private final String message;
    private final String errorUrl;
    private final long submittedRows;
    private final long submittedBytes;
    private final Long totalRows;
    private final Long loadedRows;
    private final Integer filteredRows;
    private final Integer unselectedRows;

    private BatchLoadResult(
            long firstSequence,
            long lastSequence,
            String database,
            String table,
            long submittedRows,
            long submittedBytes,
            RespContent response) {
        if (firstSequence <= 0 || lastSequence < firstSequence) {
            throw new IllegalArgumentException("Invalid load sequence range");
        }
        if (submittedRows < 0 || submittedBytes < 0) {
            throw new IllegalArgumentException(
                    "Submitted row and byte counts must be non-negative");
        }
        if (response == null) {
            throw new IllegalArgumentException("Stream Load response must not be null");
        }
        this.firstSequence = firstSequence;
        this.lastSequence = lastSequence;
        this.database = database;
        this.table = table;
        this.label = response.getLabel();
        this.transactionId = response.getTxnId();
        this.status = response.getStatus();
        this.existingJobStatus = response.getExistingJobStatus();
        this.message = response.getMessage();
        this.errorUrl = response.getErrorURL();
        this.submittedRows = submittedRows;
        this.submittedBytes = submittedBytes;
        this.totalRows = response.getNumberTotalRows();
        this.loadedRows = response.getNumberLoadedRows();
        this.filteredRows = response.getNumberFilteredRows();
        this.unselectedRows = response.getNumberUnselectedRows();
    }

    static BatchLoadResult completed(
            long firstSequence,
            long lastSequence,
            String database,
            String table,
            long submittedRows,
            long submittedBytes,
            RespContent response) {
        return new BatchLoadResult(
                firstSequence,
                lastSequence,
                database,
                table,
                submittedRows,
                submittedBytes,
                response);
    }

    public long getFirstSequence() {
        return firstSequence;
    }

    public long getLastSequence() {
        return lastSequence;
    }

    public String getDatabase() {
        return database;
    }

    public String getTable() {
        return table;
    }

    public String getLabel() {
        return label;
    }

    public Long getTransactionId() {
        return transactionId;
    }

    public String getStatus() {
        return status;
    }

    public String getExistingJobStatus() {
        return existingJobStatus;
    }

    public String getMessage() {
        return message;
    }

    public String getErrorUrl() {
        return errorUrl;
    }

    public long getSubmittedRows() {
        return submittedRows;
    }

    public long getSubmittedBytes() {
        return submittedBytes;
    }

    public Long getTotalRows() {
        return totalRows;
    }

    public Long getLoadedRows() {
        return loadedRows;
    }

    public Integer getFilteredRows() {
        return filteredRows;
    }

    public Integer getUnselectedRows() {
        return unselectedRows;
    }
}
