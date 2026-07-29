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

import org.apache.flink.api.common.time.Deadline;

import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.exception.DorisBatchLoadException;
import org.apache.doris.flink.sink.BackendUtil;
import org.apache.doris.flink.sink.HttpTestUtil;
import org.apache.doris.flink.sink.TestUtil;
import org.apache.doris.flink.sink.writer.LabelGenerator;
import org.apache.doris.flink.sink.writer.LoadConstants;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

public class DorisBatchStreamLoadFlushResultTest {

    private MockedStatic<BackendUtil> backendUtilMockedStatic;
    private DorisBatchStreamLoad loader;

    @Before
    public void setUp() {
        backendUtilMockedStatic = mockStatic(BackendUtil.class);
        backendUtilMockedStatic.when(() -> BackendUtil.tryHttpConnection(any())).thenReturn(true);
    }

    @After
    public void tearDown() {
        if (loader != null) {
            loader.close();
        }
        backendUtilMockedStatic.close();
    }

    @Test
    public void flushAndWaitReturnsOneCompletedEpochAndDrainsIt() throws Exception {
        loader = createLoader(10_000);
        configureSuccessfulHttpClient(loader);

        loader.writeRecord("db", "tbl", "one".getBytes(StandardCharsets.UTF_8));
        BatchFlushResult first = loader.flushAndWait();
        BatchFlushResult second = loader.flushAndWait();

        Assert.assertEquals(0L, first.getFromSequenceExclusive());
        Assert.assertEquals(1L, first.getThroughSequenceInclusive());
        Assert.assertEquals(1, first.getLoadResults().size());
        BatchLoadResult load = first.getLoadResults().get(0);
        Assert.assertEquals(1L, load.getFirstSequence());
        Assert.assertEquals(1L, load.getLastSequence());
        Assert.assertEquals("db", load.getDatabase());
        Assert.assertEquals("tbl", load.getTable());
        Assert.assertEquals(1L, load.getSubmittedRows());
        Assert.assertTrue(second.isEmpty());
        Assert.assertEquals(1L, second.getThroughSequenceInclusive());
        Assert.assertEquals(0, loader.getTrackedCompletionCount());
    }

    @Test
    public void preBatchedPayloadReportsLogicalRowsInsteadOfPhysicalPayloadCount()
            throws Exception {
        loader = createLoader(10_000);
        configureHttpClient(
                loader, ignored -> HttpTestUtil.getResponse(successfulResponse(7L), true));

        loader.writeRecord("db", "tbl", "one-arrow-payload".getBytes(StandardCharsets.UTF_8), 7L);
        BatchFlushResult result = loader.flushAndWait();

        Assert.assertEquals(7L, result.getSubmittedRows());
        Assert.assertEquals(
                "one-arrow-payload".getBytes(StandardCharsets.UTF_8).length,
                result.getSubmittedBytes());
        Assert.assertEquals(7L, result.getTotalRows());
        Assert.assertEquals(7L, result.getLoadedRows());
        Assert.assertEquals(1, result.getLoadResults().size());
    }

    @Test(expected = IllegalArgumentException.class)
    public void logicalRowCountMustBePositive() throws Exception {
        loader = createLoader(10_000);
        loader.writeRecord("db", "tbl", new byte[] {1}, 0L);
    }

    @Test
    public void flushAndWaitReturnsIndependentConsecutiveEpochs() throws Exception {
        loader = createLoader(10_000);
        configureSuccessfulHttpClient(loader);

        loader.writeRecord("db", "tbl", "one".getBytes(StandardCharsets.UTF_8));
        BatchFlushResult first = loader.flushAndWait();
        loader.writeRecord("db", "tbl", "two".getBytes(StandardCharsets.UTF_8));
        BatchFlushResult second = loader.flushAndWait();

        Assert.assertEquals(0L, first.getFromSequenceExclusive());
        Assert.assertEquals(1L, first.getThroughSequenceInclusive());
        Assert.assertEquals(1L, second.getFromSequenceExclusive());
        Assert.assertEquals(2L, second.getThroughSequenceInclusive());
        Assert.assertEquals(2L, second.getLoadResults().get(0).getFirstSequence());
    }

    @Test
    public void flushAndWaitIncludesEarlierFullFlushAndMultipleTables() throws Exception {
        loader = createLoader(10_000);
        configureSuccessfulHttpClient(loader);

        for (int row = 0; row < 10_000; row++) {
            loader.writeRecord("db", "first", "one".getBytes(StandardCharsets.UTF_8));
        }
        loader.writeRecord("db", "second", "two".getBytes(StandardCharsets.UTF_8));
        BatchFlushResult result = loader.flushAndWait();

        Assert.assertEquals(2L, result.getThroughSequenceInclusive());
        Assert.assertEquals(2, result.getLoadResults().size());
        Assert.assertEquals("first", result.getLoadResults().get(0).getTable());
        Assert.assertEquals("second", result.getLoadResults().get(1).getTable());
        Assert.assertEquals(
                10_001L,
                result.getLoadResults().stream()
                        .mapToLong(BatchLoadResult::getSubmittedRows)
                        .sum());
        Assert.assertEquals(0, loader.getTrackedCompletionCount());
    }

    @Test
    public void multipleSameTableBuffersCoverEverySequenceOnce() throws Exception {
        loader = createLoader(10_000);
        configureSuccessfulHttpClient(loader);

        loader.writeRecord("db", "tbl", "one".getBytes(StandardCharsets.UTF_8));
        loader.bufferFullFlush("db.tbl");
        loader.writeRecord("db", "tbl", "two".getBytes(StandardCharsets.UTF_8));
        loader.bufferFullFlush("db.tbl");
        BatchFlushResult result = loader.flushAndWait();

        List<BatchLoadResult> loads = result.getLoadResults();
        Assert.assertEquals(1L, loads.get(0).getFirstSequence());
        Assert.assertEquals(2L, loads.get(loads.size() - 1).getLastSequence());
        Assert.assertEquals(2L, loads.stream().mapToLong(BatchLoadResult::getSubmittedRows).sum());
    }

    @Test
    public void failedLoadFailsTheEpochAndClearsRetainedResults() throws Exception {
        loader = createLoader(10_000);
        configureFailingHttpClient(loader);
        loader.writeRecord("db", "tbl", "one".getBytes(StandardCharsets.UTF_8));

        try {
            loader.flushAndWait();
            Assert.fail("failed Stream Load must fail the drain epoch");
        } catch (DorisBatchLoadException expected) {
            Assert.assertTrue(expected.getMessage().contains("stream load"));
        }
        Assert.assertEquals(0, loader.getTrackedCompletionCount());
    }

    @Test
    public void concurrentDrainsProduceOneNonOverlappingEpoch() throws Exception {
        loader = createLoader(10_000);
        CountDownLatch requestStarted = new CountDownLatch(1);
        CountDownLatch releaseRequest = new CountDownLatch(1);
        configureBlockingHttpClient(loader, requestStarted, releaseRequest);
        loader.writeRecord("db", "tbl", "one".getBytes(StandardCharsets.UTF_8));
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<BatchFlushResult> first = executor.submit(loader::flushAndWait);
            Assert.assertTrue(requestStarted.await(5, TimeUnit.SECONDS));
            Future<BatchFlushResult> second = executor.submit(loader::flushAndWait);
            releaseRequest.countDown();

            Assert.assertEquals(1, first.get(5, TimeUnit.SECONDS).getLoadResults().size());
            Assert.assertTrue(second.get(5, TimeUnit.SECONDS).isEmpty());
        } finally {
            releaseRequest.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    public void closeUnblocksPendingDrainAndTerminatesTheWorker() throws Exception {
        loader = createLoader(10_000);
        CountDownLatch requestStarted = new CountDownLatch(1);
        CountDownLatch releaseRequest = new CountDownLatch(1);
        configureBlockingHttpClient(loader, requestStarted, releaseRequest);
        loader.writeRecord("db", "tbl", "one".getBytes(StandardCharsets.UTF_8));
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<BatchFlushResult> pending = executor.submit(loader::flushAndWait);
            Assert.assertTrue(requestStarted.await(5, TimeUnit.SECONDS));
            long closeStartedNanos = System.nanoTime();
            loader.close();
            Assert.assertTrue(
                    "close should interrupt an in-flight request without retry backoff",
                    Duration.ofNanos(System.nanoTime() - closeStartedNanos)
                                    .compareTo(Duration.ofMillis(500))
                            < 0);
            try {
                pending.get(5, TimeUnit.SECONDS);
                Assert.fail("close must fail a pending drain");
            } catch (ExecutionException expected) {
                Assert.assertTrue(expected.getCause() instanceof DorisBatchLoadException);
            }
            Assert.assertFalse(loader.isLoadThreadAlive());
            Assert.assertEquals(0, loader.getTrackedCompletionCount());
        } finally {
            releaseRequest.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    public void closeCannotLeaveABufferEnqueuedByABlockedProducer() throws Exception {
        loader = createLoader(10_000, 1);
        CountDownLatch requestStarted = new CountDownLatch(1);
        CountDownLatch releaseRequest = new CountDownLatch(1);
        configureBlockingHttpClient(loader, requestStarted, releaseRequest);
        loader.writeRecord("db", "first", "one".getBytes(StandardCharsets.UTF_8));
        Assert.assertTrue(loader.bufferFullFlush("db.first"));
        Assert.assertTrue(requestStarted.await(5, TimeUnit.SECONDS));
        loader.writeRecord("db", "second", "two".getBytes(StandardCharsets.UTF_8));
        Assert.assertTrue(loader.bufferFullFlush("db.second"));
        loader.writeRecord("db", "third", "three".getBytes(StandardCharsets.UTF_8));

        ExecutorService executor = Executors.newSingleThreadExecutor();
        CountDownLatch producerStarted = new CountDownLatch(1);
        try {
            Future<Boolean> blockedProducer =
                    executor.submit(
                            () -> {
                                producerStarted.countDown();
                                return loader.bufferFullFlush("db.third");
                            });
            Assert.assertTrue(producerStarted.await(5, TimeUnit.SECONDS));
            Thread.sleep(100L);
            Assert.assertFalse(
                    "producer did not block on the full queue", blockedProducer.isDone());
            loader.close();
            try {
                blockedProducer.get(5, TimeUnit.SECONDS);
                Assert.fail("a producer racing close must fail");
            } catch (ExecutionException expected) {
                Assert.assertTrue(expected.getCause() instanceof DorisBatchLoadException);
            }
            Assert.assertEquals(0, loader.getPendingFlushCount());
        } finally {
            releaseRequest.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    public void completionTrackingAppliesBackpressureUntilDrain() throws Exception {
        loader = createLoader(10_000, 1);
        configureSuccessfulHttpClient(loader);

        loader.writeRecord("db", "first", "one".getBytes(StandardCharsets.UTF_8));
        Assert.assertTrue(loader.bufferFullFlush("db.first"));
        TestUtil.waitUntilCondition(
                () -> loader.getTrackedCompletionCount() == 1,
                Deadline.fromNow(Duration.ofSeconds(5)),
                10L,
                "first completion was not retained");

        loader.writeRecord("db", "second", "two".getBytes(StandardCharsets.UTF_8));
        Assert.assertTrue(loader.bufferFullFlush("db.second"));
        Thread.sleep(100L);
        Assert.assertEquals(
                "completion retention exceeded queue capacity",
                1,
                loader.getTrackedCompletionCount());

        loader.writeRecord("db", "third", "three".getBytes(StandardCharsets.UTF_8));
        Assert.assertTrue(loader.bufferFullFlush("db.third"));
        BatchFlushResult result = loader.flushAndWait();

        Assert.assertEquals(3L, result.getThroughSequenceInclusive());
        Assert.assertEquals(3, result.getLoadResults().size());
        Assert.assertEquals(0, loader.getTrackedCompletionCount());
    }

    @Test
    public void visibleFlushRejectsAsyncGroupCommitBeforeSubmittingBufferedRows() throws Exception {
        Properties properties = new Properties();
        properties.setProperty(LoadConstants.GROUP_COMMIT, LoadConstants.GROUP_COMMIT_ASYNC_MODE);
        loader = createLoader(10_000, 8, properties);
        configureSuccessfulHttpClient(loader);
        loader.writeRecord("db", "tbl", "one".getBytes(StandardCharsets.UTF_8));

        try {
            loader.flushAndWait();
            Assert.fail("async group commit must not produce visible flush results");
        } catch (DorisBatchLoadException expected) {
            Assert.assertTrue(expected.getMessage().contains("async_mode"));
        }
        Assert.assertEquals(0, loader.getPendingFlushCount());
    }

    @Test
    public void visibleFlushExplicitlySupportsOffMode() throws Exception {
        Properties properties = new Properties();
        properties.setProperty(LoadConstants.GROUP_COMMIT, LoadConstants.GROUP_COMMIT_OFF_MODE);
        loader = createLoader(10_000, 8, properties);
        configureSuccessfulHttpClient(loader);
        loader.writeRecord("db", "tbl", "one".getBytes(StandardCharsets.UTF_8));

        BatchLoadResult result = loader.flushAndWait().getLoadResults().get(0);

        Assert.assertEquals("VISIBLE", result.getTransactionStatus());
    }

    @Test
    public void writeRecordWithSizeReturnsExactBufferedBytesIncludingDelimiter() throws Exception {
        loader = createLoader(10_000, 8);

        Assert.assertEquals(
                3,
                loader.writeRecordAndGetBufferedBytes(
                        "db", "tbl", "one".getBytes(StandardCharsets.UTF_8), 1L));
        Assert.assertEquals(
                4,
                loader.writeRecordAndGetBufferedBytes(
                        "db", "tbl", "two".getBytes(StandardCharsets.UTF_8), 1L));
    }

    private DorisBatchStreamLoad createLoader(int maxRows) throws Exception {
        return createLoader(maxRows, 8);
    }

    private DorisBatchStreamLoad createLoader(int maxRows, int queueSize) throws Exception {
        return createLoader(maxRows, queueSize, new Properties());
    }

    private DorisBatchStreamLoad createLoader(
            int maxRows, int queueSize, Properties streamLoadProperties) throws Exception {
        streamLoadProperties.putIfAbsent(LoadConstants.COMPRESS_TYPE, "none");
        DorisExecutionOptions executionOptions =
                DorisExecutionOptions.builder()
                        .setBufferFlushMaxRows(maxRows)
                        .setBufferFlushIntervalMs(60_000)
                        .setFlushQueueSize(queueSize)
                        .setMaxRetries(0)
                        .setStreamLoadProp(streamLoadProperties)
                        .build();
        DorisOptions options =
                DorisOptions.builder()
                        .setFenodes("127.0.0.1:8030")
                        .setBenodes("127.0.0.1:8040")
                        .setTableIdentifier("db.tbl")
                        .build();
        DorisBatchStreamLoad result =
                new DorisBatchStreamLoad(
                        options,
                        DorisReadOptions.builder().build(),
                        executionOptions,
                        new LabelGenerator("flush-result", false),
                        0);
        TestUtil.waitUntilCondition(
                result::isLoadThreadAlive,
                Deadline.fromNow(Duration.ofSeconds(5)),
                10L,
                "load worker did not start");
        return result;
    }

    private void configureSuccessfulHttpClient(DorisBatchStreamLoad target) throws Exception {
        configureHttpClient(
                target,
                invocation -> {
                    HttpPut request = invocation.getArgument(0);
                    BatchBufferHttpEntity entity = (BatchBufferHttpEntity) request.getEntity();
                    long rows = entity.getRecordCount();
                    return HttpTestUtil.getResponse(successfulResponse(rows), true);
                });
    }

    private void configureFailingHttpClient(DorisBatchStreamLoad target) throws Exception {
        configureHttpClient(target, ignored -> HttpTestUtil.getResponse("server error", false));
    }

    private void configureBlockingHttpClient(
            DorisBatchStreamLoad target,
            CountDownLatch requestStarted,
            CountDownLatch releaseRequest)
            throws Exception {
        configureHttpClient(
                target,
                ignored -> {
                    requestStarted.countDown();
                    if (!releaseRequest.await(5, TimeUnit.SECONDS)) {
                        throw new IllegalStateException("test did not release Stream Load request");
                    }
                    return HttpTestUtil.getResponse(successfulResponse(1L), true);
                });
    }

    private void configureHttpClient(
            DorisBatchStreamLoad target,
            org.mockito.stubbing.Answer<org.apache.http.client.methods.CloseableHttpResponse>
                    answer)
            throws Exception {
        BackendUtil backendUtil = mock(BackendUtil.class);
        when(backendUtil.getAvailableBackend(anyInt())).thenReturn("127.0.0.1:8040");
        HttpClientBuilder httpClientBuilder = mock(HttpClientBuilder.class);
        CloseableHttpClient httpClient = mock(CloseableHttpClient.class);
        when(httpClientBuilder.build()).thenReturn(httpClient);
        when(httpClient.execute(any())).thenAnswer(answer);
        target.setBackendUtil(backendUtil);
        target.setHttpClientBuilder(httpClientBuilder);
    }

    private String successfulResponse(long rows) {
        return "{\"TxnId\":9,\"Label\":\"result-label\","
                + "\"Status\":\"Success\",\"Message\":\"OK\","
                + "\"NumberTotalRows\":"
                + rows
                + ",\"NumberLoadedRows\":"
                + rows
                + ","
                + "\"NumberFilteredRows\":0,"
                + "\"NumberUnselectedRows\":0,\"LoadBytes\":3}";
    }
}
