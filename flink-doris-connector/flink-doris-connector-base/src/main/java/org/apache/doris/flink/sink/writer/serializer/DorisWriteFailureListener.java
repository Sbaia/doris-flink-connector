package org.apache.doris.flink.sink.writer.serializer;

import java.io.Serializable;

/**
 * Notified when {@link RowDataSerializer} fails to serialize a buffered Arrow batch and drops it
 * (see {@link RowDataSerializer#arrowToDorisRecord()}).
 *
 * <p>The serializer has no side-output or DLQ mechanism of its own — it runs inside a Sink2 {@code
 * SinkWriter}, which has no {@code Collector}/side-output access. This seam lets a caller (e.g. a
 * job-level DLQ publisher) observe the failure without changing the serializer's drop-and-continue
 * fallback: the batch is still dropped so a poison-pill batch can never fail the job, this listener
 * only adds visibility on top of that existing behavior.
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
