/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.doris.flink.sink.writer.arrow;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;

import org.apache.doris.flink.sink.writer.LoadConstants;
import org.apache.doris.flink.sink.writer.serializer.DorisRecord;
import org.apache.doris.flink.sink.writer.serializer.RowDataSerializer;
import org.junit.Assert;
import org.junit.Test;

import java.util.ServiceLoader;

/** Tests the Flink 2-specific Arrow ZSTD provider and its service registration. */
public class Flink2ZstdArrowStreamWriterProviderTest {

    @Test
    public void serviceLoaderProvidesCompressedArrowWriter() throws Exception {
        ZstdArrowStreamWriterProvider provider =
                ServiceLoader.load(
                                ZstdArrowStreamWriterProvider.class,
                                ZstdArrowStreamWriterProvider.class.getClassLoader())
                        .iterator()
                        .next();
        Assert.assertEquals(Flink2ZstdArrowStreamWriterProvider.class, provider.getClass());

        RowDataSerializer serializer =
                RowDataSerializer.builder()
                        .setFieldNames(new String[] {"value"})
                        .setFieldType(
                                new org.apache.flink.table.types.DataType[] {DataTypes.STRING()})
                        .setType(LoadConstants.ARROW)
                        .enableArrowCompression(true)
                        .build();
        serializer.initial();
        try {
            serializer.serialize(
                    GenericRowData.of(
                            StringData.fromString(
                                    "compressible-compressible-compressible-compressible")));
            DorisRecord record = serializer.flush();
            Assert.assertNotSame(DorisRecord.empty, record);
            Assert.assertTrue(record.getRow().length > 0);
        } finally {
            serializer.close();
        }
    }
}
