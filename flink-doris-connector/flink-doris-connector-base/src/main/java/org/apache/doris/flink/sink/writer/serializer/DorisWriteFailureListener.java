// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.flink.sink.writer.serializer;

import java.io.Serializable;

/**
 * Notified when {@link RowDataSerializer} fails to serialize a buffered Arrow batch and drops it
 * (see {@link RowDataSerializer#arrowToDorisRecord()}).
 *
 * <p>The serializer runs inside a Sink2 {@code SinkWriter}, which has no {@code Collector} access.
 * This seam lets a caller observe the failure without changing the serializer's existing
 * drop-and-continue fallback: the batch is still dropped so a poison-pill batch cannot repeatedly
 * fail the job, while the listener makes that outcome observable.
 *
 * <p>Implementations must not throw: {@link RowDataSerializer} invokes the listener from within its
 * own failure-recovery catch block and swallows any exception the listener raises, but a
 * well-behaved implementation should handle its own errors internally regardless.
 */
public interface DorisWriteFailureListener extends Serializable {

    /**
     * Called when an Arrow batch fails to serialize and is dropped.
     *
     * @param recordCount number of records that were buffered in the failed batch
     * @param cause the exception that caused the failure
     */
    void onArrowBatchFailure(int recordCount, Exception cause);

    /** No-op listener used when no failure notification is configured. */
    DorisWriteFailureListener NOOP = (recordCount, cause) -> {};
}
