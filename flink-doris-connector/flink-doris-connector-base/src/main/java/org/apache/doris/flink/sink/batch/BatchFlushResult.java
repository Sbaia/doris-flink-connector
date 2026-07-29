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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Immutable results for one non-overlapping synchronous flush epoch. */
public final class BatchFlushResult implements Serializable {
    private static final long serialVersionUID = 1L;

    private final long fromSequenceExclusive;
    private final long throughSequenceInclusive;
    private final List<BatchLoadResult> loadResults;
    private final long submittedRows;
    private final long totalRows;
    private final long loadedRows;
    private final long filteredRows;
    private final long unselectedRows;

    private BatchFlushResult(
            long fromSequenceExclusive,
            long throughSequenceInclusive,
            List<BatchLoadResult> loadResults) {
        if (fromSequenceExclusive < 0 || throughSequenceInclusive < fromSequenceExclusive) {
            throw new IllegalArgumentException("Invalid flush sequence range");
        }
        if (loadResults == null) {
            throw new IllegalArgumentException("Load results must not be null");
        }
        this.fromSequenceExclusive = fromSequenceExclusive;
        this.throughSequenceInclusive = throughSequenceInclusive;
        this.loadResults =
                Collections.unmodifiableList(new ArrayList<BatchLoadResult>(loadResults));
        this.submittedRows = sum(loadResults, BatchLoadResult::getSubmittedRows);
        this.totalRows = sum(loadResults, result -> result.getTotalRows());
        this.loadedRows = sum(loadResults, result -> result.getLoadedRows());
        this.filteredRows = sum(loadResults, result -> result.getFilteredRows());
        this.unselectedRows = sum(loadResults, result -> result.getUnselectedRows());
    }

    static BatchFlushResult completed(
            long fromSequenceExclusive,
            long throughSequenceInclusive,
            List<BatchLoadResult> loadResults) {
        return new BatchFlushResult(fromSequenceExclusive, throughSequenceInclusive, loadResults);
    }

    static BatchFlushResult empty(long watermark) {
        return new BatchFlushResult(watermark, watermark, Collections.<BatchLoadResult>emptyList());
    }

    public long getFromSequenceExclusive() {
        return fromSequenceExclusive;
    }

    public long getThroughSequenceInclusive() {
        return throughSequenceInclusive;
    }

    public List<BatchLoadResult> getLoadResults() {
        return loadResults;
    }

    public long getSubmittedRows() {
        return submittedRows;
    }

    public long getTotalRows() {
        return totalRows;
    }

    public long getLoadedRows() {
        return loadedRows;
    }

    public long getFilteredRows() {
        return filteredRows;
    }

    public long getUnselectedRows() {
        return unselectedRows;
    }

    public boolean isEmpty() {
        return loadResults.isEmpty();
    }

    private static long sum(List<BatchLoadResult> results, LongValue value) {
        long total = 0L;
        for (BatchLoadResult result : results) {
            Number number = value.get(result);
            if (number == null) {
                throw new IllegalArgumentException("Completed load result is missing row counts");
            }
            total = Math.addExact(total, number.longValue());
        }
        return total;
    }

    @FunctionalInterface
    private interface LongValue {
        Number get(BatchLoadResult result);
    }
}
