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

import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.BackendUtil;
import org.apache.doris.flink.sink.HttpTestUtil;
import org.apache.doris.flink.sink.writer.serializer.DorisRecord;
import org.apache.doris.flink.sink.writer.serializer.SimpleStringSerializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.MockedStatic;

import java.lang.reflect.Field;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

public class TestDorisBatchWriter {

    @Rule public ExpectedException thrown = ExpectedException.none();

    private MockedStatic<BackendUtil> backendUtilMockedStatic;

    @Before
    public void setUp() throws Exception {
        backendUtilMockedStatic = mockStatic(BackendUtil.class);
        backendUtilMockedStatic.when(() -> BackendUtil.tryHttpConnection(any())).thenReturn(true);
    }

    @Test
    public void testInit() {
        DorisOptions options =
                DorisOptions.builder()
                        .setFenodes("127.0.0.1:8030")
                        .setTableIdentifier("db.tbl.a")
                        .build();
        thrown.expect(IllegalStateException.class);
        thrown.expectMessage("tableIdentifier input error");
        DorisBatchWriter batchWriter = new DorisBatchWriter(1, 1, null, options, null, null);
    }

    @Test
    public void testWriteOneDorisRecord() throws Exception {
        DorisOptions options =
                DorisOptions.builder()
                        .setFenodes("127.0.0.1:8030")
                        .setTableIdentifier("db.tbl")
                        .build();
        DorisReadOptions readOptions = DorisReadOptions.builder().build();
        DorisExecutionOptions executionOptions = DorisExecutionOptions.builder().build();
        SimpleStringSerializer simpleStringSerializer = new SimpleStringSerializer();
        DorisBatchWriter batchWriter =
                new DorisBatchWriter(
                        1, 1, simpleStringSerializer, options, readOptions, executionOptions);
        batchWriter.writeOneDorisRecord(null);
        batchWriter.writeOneDorisRecord(DorisRecord.of(null));
        batchWriter.writeOneDorisRecord(DorisRecord.of("db", "tbl", "zhangsan,1".getBytes()));
        batchWriter.close();
    }

    @Test
    public void flushAndWaitReturnsTheUnderlyingCompletedEpoch() throws Exception {
        DorisOptions options =
                DorisOptions.builder()
                        .setFenodes("127.0.0.1:8030")
                        .setBenodes("127.0.0.1:8040")
                        .setTableIdentifier("db.tbl")
                        .build();
        DorisBatchWriter<String> batchWriter =
                new DorisBatchWriter<>(
                        1,
                        1,
                        new SimpleStringSerializer(),
                        options,
                        DorisReadOptions.builder().build(),
                        DorisExecutionOptions.builder().setMaxRetries(0).build());
        try {
            DorisBatchStreamLoad loader = batchStreamLoadOf(batchWriter);
            BackendUtil backendUtil = mock(BackendUtil.class);
            when(backendUtil.getAvailableBackend(anyInt())).thenReturn("127.0.0.1:8040");
            org.apache.http.impl.client.HttpClientBuilder httpClientBuilder =
                    mock(org.apache.http.impl.client.HttpClientBuilder.class);
            org.apache.http.impl.client.CloseableHttpClient httpClient =
                    mock(org.apache.http.impl.client.CloseableHttpClient.class);
            when(httpClientBuilder.build()).thenReturn(httpClient);
            org.apache.http.client.methods.CloseableHttpResponse response =
                    HttpTestUtil.getResponse(
                            "{\"TxnId\":9,\"Label\":\"writer-label\","
                                    + "\"Status\":\"Success\",\"Message\":\"OK\","
                                    + "\"NumberTotalRows\":1,\"NumberLoadedRows\":1,"
                                    + "\"NumberFilteredRows\":0,"
                                    + "\"NumberUnselectedRows\":0,\"LoadBytes\":3}",
                            true);
            when(httpClient.execute(any())).thenReturn(response);
            loader.setBackendUtil(backendUtil);
            loader.setHttpClientBuilder(httpClientBuilder);

            batchWriter.writeOneDorisRecord(DorisRecord.of("one".getBytes()));
            BatchFlushResult result = batchWriter.flushAndWait();

            org.junit.Assert.assertEquals(1, result.getLoadResults().size());
            org.junit.Assert.assertEquals(
                    "writer-label", result.getLoadResults().get(0).getLabel());
        } finally {
            batchWriter.close();
        }
    }

    private DorisBatchStreamLoad batchStreamLoadOf(DorisBatchWriter<?> writer) throws Exception {
        Field field = DorisBatchWriter.class.getDeclaredField("batchStreamLoad");
        field.setAccessible(true);
        return (DorisBatchStreamLoad) field.get(writer);
    }

    @After
    public void after() {
        if (backendUtilMockedStatic != null) {
            backendUtilMockedStatic.close();
        }
    }
}
