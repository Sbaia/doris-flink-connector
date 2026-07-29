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

package org.apache.doris.flink.sink.writer.serializer;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Preconditions;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.doris.flink.deserialization.converter.DorisRowConverter;
import org.apache.doris.flink.sink.EscapeHandler;
import org.apache.doris.flink.sink.writer.arrow.ArrowSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.StringJoiner;

import static org.apache.doris.flink.sink.writer.LoadConstants.ARROW;
import static org.apache.doris.flink.sink.writer.LoadConstants.CSV;
import static org.apache.doris.flink.sink.writer.LoadConstants.DORIS_DELETE_SIGN;
import static org.apache.doris.flink.sink.writer.LoadConstants.JSON;
import static org.apache.doris.flink.sink.writer.LoadConstants.NULL_VALUE;

/** Serializer for RowData. */
@PublicEvolving
public class RowDataSerializer implements DorisRecordSerializer<RowData> {
    private static final Logger LOG = LoggerFactory.getLogger(RowDataSerializer.class);
    String[] fieldNames;
    String type;
    private ObjectMapper objectMapper;
    private final String fieldDelimiter;
    private final boolean enableDelete;
    private final DorisRowConverter rowConverter;
    private ArrowSerializer arrowSerializer;
    ByteArrayOutputStream outputStream;
    private final int arrowBatchCnt = 1000;
    private int arrowWriteCnt = 0;
    private final DataType[] dataTypes;
    private final boolean arrowCompression;
    private final DorisWriteFailureListener failureListener;

    private RowDataSerializer(
            String[] fieldNames,
            DataType[] dataTypes,
            String type,
            String fieldDelimiter,
            boolean enableDelete,
            boolean arrowCompression,
            DorisWriteFailureListener failureListener) {
        this.fieldNames = fieldNames;
        this.type = type;
        this.fieldDelimiter = fieldDelimiter;
        this.enableDelete = enableDelete;
        this.arrowCompression = arrowCompression;
        this.failureListener =
                failureListener != null ? failureListener : DorisWriteFailureListener.NOOP;
        if (JSON.equals(type)) {
            objectMapper = new ObjectMapper();
        }
        this.dataTypes = dataTypes;
        this.rowConverter = new DorisRowConverter().setExternalConverter(dataTypes);
    }

    @Override
    public void initial() {
        if (ARROW.equals(type)) {
            LogicalType[] logicalTypes = TypeConversions.fromDataToLogicalType(dataTypes);
            RowType rowType = RowType.of(logicalTypes, fieldNames);
            arrowSerializer = new ArrowSerializer(rowType, rowType, arrowCompression);
            outputStream = new ByteArrayOutputStream();
            try {
                arrowSerializer.open(new ByteArrayInputStream(new byte[0]), outputStream);
            } catch (Exception e) {
                throw new RuntimeException("failed to open arrow serializer:", e);
            }
        }
    }

    @Override
    public DorisRecord serialize(RowData record) throws IOException {
        int maxIndex = Math.min(record.getArity(), fieldNames.length);
        String valString;
        if (JSON.equals(type)) {
            valString = buildJsonString(record, maxIndex);
        } else if (CSV.equals(type)) {
            valString = buildCSVString(record, maxIndex);
        } else if (ARROW.equals(type)) {
            arrowWriteCnt += 1;
            arrowSerializer.write(record);
            if (arrowWriteCnt < arrowBatchCnt) {
                return DorisRecord.empty;
            }
            return arrowToDorisRecord();
        } else {
            throw new IllegalArgumentException("The type " + type + " is not supported!");
        }
        return DorisRecord.of(valString.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public DorisRecord flush() {
        if (JSON.equals(type) || CSV.equals(type)) {
            return DorisRecord.empty;
        } else if (ARROW.equals(type)) {
            return arrowToDorisRecord();
        } else {
            throw new IllegalArgumentException("The type " + type + " is not supported!");
        }
    }

    /**
     * Returns the number of rows currently buffered by the Arrow serializer. Non-Arrow formats
     * never buffer rows and therefore return zero.
     */
    public int getBufferedRowCount() {
        return ARROW.equals(type) ? arrowWriteCnt : 0;
    }

    /**
     * Drops and resets the current Arrow batch after a deterministic row-write failure. The same
     * listener contract as a batch-finalization failure is used so callers can account for every
     * buffered row exactly once.
     */
    public void discardBufferedArrowBatch(Exception cause) {
        Preconditions.checkNotNull(cause, "cause must not be null");
        if (!ARROW.equals(type) || arrowWriteCnt == 0) {
            return;
        }
        int batchSize = arrowWriteCnt;
        arrowWriteCnt = 0;
        recoverDroppedArrowBatch(batchSize, cause);
    }

    @Override
    public void close() throws Exception {
        if (ARROW.equals(type)) {
            arrowSerializer.close();
        }
    }

    // Batch is dropped on Arrow serialization failure rather than rethrown, so a
    // poison-pill batch can never fail the job. The failure listener makes the
    // drop observable; this method's own fallback stays drop-and-recover regardless
    // of what the listener does.
    public DorisRecord arrowToDorisRecord() {
        if (arrowWriteCnt == 0) {
            return DorisRecord.empty;
        }
        int batchSize = arrowWriteCnt;
        arrowWriteCnt = 0;
        try {
            arrowSerializer.finishCurrentBatch();
            byte[] bytes = outputStream.toByteArray();
            outputStream.reset();
            arrowSerializer.resetWriter();
            return DorisRecord.of(bytes);
        } catch (Exception e) {
            LOG.error("Arrow batch serialization failed, dropping {} records", batchSize, e);
            recoverDroppedArrowBatch(batchSize, e);
        }
        return DorisRecord.empty;
    }

    private void recoverDroppedArrowBatch(int batchSize, Exception cause) {
        try {
            outputStream.reset();
            arrowSerializer.resetWriter();
        } catch (Exception resetFailure) {
            LOG.error("Failed to reset Arrow writer after serialization failure", resetFailure);
        }
        notifyFailureListener(batchSize, cause);
    }

    // The failure listener must never break the drop-and-recover fallback above,
    // so a misbehaving listener is caught and logged, not propagated.
    private void notifyFailureListener(int batchSize, Exception cause) {
        try {
            failureListener.onArrowBatchFailure(batchSize, cause);
        } catch (Exception listenerEx) {
            LOG.error(
                    "Doris write failure listener threw while handling a dropped batch of {} records",
                    batchSize,
                    listenerEx);
        }
    }

    public String buildJsonString(RowData record, int maxIndex) throws IOException {
        int fieldIndex = 0;
        Map<String, String> valueMap = new HashMap<>();
        while (fieldIndex < maxIndex) {
            Object field = rowConverter.convertExternal(record, fieldIndex);
            String value = stringifyField(field);
            valueMap.put(fieldNames[fieldIndex], value);
            fieldIndex++;
        }
        if (enableDelete) {
            valueMap.put(DORIS_DELETE_SIGN, parseDeleteSign(record.getRowKind()));
        }
        return objectMapper.writeValueAsString(valueMap);
    }

    public String buildCSVString(RowData record, int maxIndex) throws IOException {
        int fieldIndex = 0;
        StringJoiner joiner = new StringJoiner(fieldDelimiter);
        while (fieldIndex < maxIndex) {
            Object field = rowConverter.convertExternal(record, fieldIndex);
            String value = field != null ? stringifyField(field) : NULL_VALUE;
            joiner.add(value);
            fieldIndex++;
        }
        if (enableDelete) {
            joiner.add(parseDeleteSign(record.getRowKind()));
        }
        return joiner.toString();
    }

    private static String stringifyField(Object field) {
        if (field == null) {
            return null;
        }
        if (field instanceof byte[] bytes) {
            return Base64.getEncoder().encodeToString(bytes);
        }
        return field.toString();
    }

    public String parseDeleteSign(RowKind rowKind) {
        if (RowKind.INSERT.equals(rowKind) || RowKind.UPDATE_AFTER.equals(rowKind)) {
            return "0";
        } else if (RowKind.DELETE.equals(rowKind) || RowKind.UPDATE_BEFORE.equals(rowKind)) {
            return "1";
        } else {
            throw new IllegalArgumentException("Unrecognized row kind:" + rowKind.toString());
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    /** Builder for RowDataSerializer. */
    public static class Builder {
        private String[] fieldNames;
        private DataType[] dataTypes;
        private String type;
        private String fieldDelimiter;
        private boolean deletable;
        private boolean arrowCompression;
        private DorisWriteFailureListener failureListener;

        public Builder setFieldNames(String[] fieldNames) {
            this.fieldNames = fieldNames;
            return this;
        }

        public Builder setFieldType(DataType[] dataTypes) {
            this.dataTypes = dataTypes;
            return this;
        }

        public Builder setType(String type) {
            this.type = type;
            return this;
        }

        public Builder setFieldDelimiter(String fieldDelimiter) {
            this.fieldDelimiter = EscapeHandler.escapeString(fieldDelimiter);
            return this;
        }

        public Builder enableDelete(boolean deletable) {
            this.deletable = deletable;
            return this;
        }

        public Builder enableArrowCompression(boolean arrowCompression) {
            this.arrowCompression = arrowCompression;
            return this;
        }

        /**
         * Sets the listener notified when an Arrow batch fails to serialize and is dropped.
         * Defaults to {@link DorisWriteFailureListener#NOOP} when not set.
         */
        public Builder setFailureListener(DorisWriteFailureListener failureListener) {
            this.failureListener = failureListener;
            return this;
        }

        public RowDataSerializer build() {
            Preconditions.checkState(
                    CSV.equals(type) && fieldDelimiter != null
                            || JSON.equals(type)
                            || ARROW.equals(type));
            Preconditions.checkNotNull(dataTypes);
            Preconditions.checkNotNull(fieldNames);
            if (ARROW.equals(type)) {
                Preconditions.checkArgument(!deletable);
            }
            return new RowDataSerializer(
                    fieldNames,
                    dataTypes,
                    type,
                    fieldDelimiter,
                    deletable,
                    arrowCompression,
                    failureListener);
        }
    }
}
