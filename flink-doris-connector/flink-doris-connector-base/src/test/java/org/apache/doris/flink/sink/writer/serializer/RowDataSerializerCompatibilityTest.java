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

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.DataType;

import org.apache.doris.flink.sink.writer.LoadConstants;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.concurrent.atomic.AtomicInteger;

/** Compatibility tests for pipeline-used serializer capabilities. */
public class RowDataSerializerCompatibilityTest {

    @Test
    public void binaryFieldsUseStableBase64InCsvAndJson() throws Exception {
        byte[] value = new byte[] {0, 1, 2, 3, 4};
        DataType[] types = new DataType[] {DataTypes.BYTES()};
        GenericRowData row = GenericRowData.of(value);
        String expected = Base64.getEncoder().encodeToString(value);

        RowDataSerializer csv =
                RowDataSerializer.builder()
                        .setFieldNames(new String[] {"payload"})
                        .setFieldType(types)
                        .setType(LoadConstants.CSV)
                        .setFieldDelimiter("|")
                        .build();
        csv.initial();

        RowDataSerializer json =
                RowDataSerializer.builder()
                        .setFieldNames(new String[] {"payload"})
                        .setFieldType(types)
                        .setType(LoadConstants.JSON)
                        .build();
        json.initial();

        Assert.assertEquals(expected, csv.buildCSVString(row, 1));
        Assert.assertTrue(json.buildJsonString(row, 1).contains(expected));
    }

    @Test
    public void arrowCompressionAndDropAccountingRemainAvailable() throws Exception {
        AtomicInteger failedRows = new AtomicInteger();
        RowDataSerializer serializer =
                RowDataSerializer.builder()
                        .setFieldNames(new String[] {"value"})
                        .setFieldType(new DataType[] {DataTypes.STRING()})
                        .setType(LoadConstants.ARROW)
                        .enableArrowCompression(true)
                        .setFailureListener((rowCount, failure) -> failedRows.addAndGet(rowCount))
                        .build();
        serializer.initial();

        serializer.serialize(
                GenericRowData.of(StringData.fromString("compressible-compressible-compressible")));
        Assert.assertEquals(1, serializer.getBufferedRowCount());

        serializer.discardBufferedArrowBatch(new IllegalArgumentException("deterministic"));
        Assert.assertEquals(0, serializer.getBufferedRowCount());
        Assert.assertEquals(1, failedRows.get());

        serializer.serialize(GenericRowData.of(StringData.fromString("next")));
        DorisRecord record = serializer.flush();
        Assert.assertNotSame(DorisRecord.empty, record);
        Assert.assertTrue(new String(record.getRow(), StandardCharsets.ISO_8859_1).length() > 0);
        serializer.close();
    }
}
