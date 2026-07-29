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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.doris.flink.exception.DorisBatchLoadException;
import org.apache.doris.flink.rest.models.RespContent;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;
import java.util.concurrent.atomic.AtomicLong;

public class BatchLoadOutcomeValidatorTest {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Test
    public void successRequiresExactCountsAndProducesVisibleSanitizedResult() throws Exception {
        RespContent response =
                response(
                        "{\"TxnId\":9,\"Label\":\"safe-label\",\"Status\":\"Success\","
                                + "\"Message\":\"done\\nwithout control bytes\","
                                + "\"ErrorURL\":\"https://user:secret@doris/errors/1?token=hidden\","
                                + "\"NumberTotalRows\":2,\"NumberLoadedRows\":2,"
                                + "\"NumberFilteredRows\":0,\"NumberUnselectedRows\":0}");

        BatchLoadResult result = validator(states()).validate(1, 1, "db", "tbl", 2, 10, response);

        Assert.assertEquals("VISIBLE", result.getTransactionStatus());
        Assert.assertEquals(Long.valueOf(2), result.getTotalRows());
        Assert.assertEquals(Long.valueOf(2), result.getLoadedRows());
        Assert.assertFalse(result.getMessage().contains("\n"));
        Assert.assertEquals("https://doris/errors/1", result.getErrorUrl());
    }

    @Test
    public void publishTimeoutPollsUntilVisible() throws Exception {
        BatchLoadResult result =
                validator(states("PREPARE", "COMMITTED", "VISIBLE"))
                        .validate(1, 1, "db", "tbl", 1, 3, publishTimeout());

        Assert.assertEquals("VISIBLE", result.getTransactionStatus());
        Assert.assertEquals("Publish Timeout", result.getStatus());
    }

    @Test
    public void publishTimeoutRejectsAbortedUnknownAndVisibilityTimeout() throws Exception {
        assertRejected(states("ABORTED"), "ABORTED");
        assertRejected(states("UNKNOWN"), "UNKNOWN");
        assertRejected(states("COMMITTED", "COMMITTED", "COMMITTED"), "visibility timeout");
    }

    @Test
    public void rejectsMissingOrInexactCountsAndMalformedResponse() throws Exception {
        assertInvalid(success(2, 1, 0, 0), 2, "loaded rows");
        assertInvalid(success(2, 2, 1, 0), 2, "filtered rows");
        assertInvalid(success(2, 2, 0, 1), 2, "unselected rows");
        assertInvalid(success(1, 1, 0, 0), 2, "total rows");
        assertInvalid(response("{\"Status\":\"Success\"}"), 1, "missing");
        assertInvalid(null, 1, "missing");
    }

    @Test
    public void rejectsAsyncGroupCommitButAcceptsOffAndSyncModes() {
        BatchLoadOutcomeValidator.validateVisibleAckGroupCommitMode("off_mode");
        BatchLoadOutcomeValidator.validateVisibleAckGroupCommitMode("sync_mode");
        BatchLoadOutcomeValidator.validateVisibleAckGroupCommitMode(null);

        try {
            BatchLoadOutcomeValidator.validateVisibleAckGroupCommitMode("async_mode");
            Assert.fail("async group commit must not be used for a visible ACK");
        } catch (DorisBatchLoadException expected) {
            Assert.assertTrue(expected.getMessage().contains("async_mode"));
        }
    }

    private void assertRejected(Deque<String> states, String expectedMessage) throws Exception {
        try {
            validator(states).validate(1, 1, "db", "tbl", 1, 3, publishTimeout());
            Assert.fail("non-visible transaction must fail validation");
        } catch (DorisBatchLoadException expected) {
            Assert.assertTrue(
                    expected.getMessage(), expected.getMessage().contains(expectedMessage));
        }
    }

    private void assertInvalid(RespContent response, long submittedRows, String expectedMessage)
            throws Exception {
        try {
            validator(states()).validate(1, 1, "db", "tbl", submittedRows, 3, response);
            Assert.fail("invalid row outcome must fail validation");
        } catch (DorisBatchLoadException expected) {
            Assert.assertTrue(
                    expected.getMessage(), expected.getMessage().contains(expectedMessage));
        }
    }

    private BatchLoadOutcomeValidator validator(Deque<String> transactionStates) {
        AtomicLong now = new AtomicLong();
        return new BatchLoadOutcomeValidator(
                (database, label) -> {
                    if (transactionStates.isEmpty()) {
                        return "COMMITTED";
                    }
                    return transactionStates.removeFirst();
                },
                millis -> now.addAndGet(millis),
                now::get,
                2,
                5);
    }

    private Deque<String> states(String... values) {
        return new ArrayDeque<>(Arrays.asList(values));
    }

    private RespContent publishTimeout() throws Exception {
        return response(
                "{\"TxnId\":9,\"Label\":\"publish-label\",\"Status\":\"Publish Timeout\","
                        + "\"Message\":\"waiting\",\"NumberTotalRows\":1,"
                        + "\"NumberLoadedRows\":1,\"NumberFilteredRows\":0,"
                        + "\"NumberUnselectedRows\":0}");
    }

    private RespContent success(long total, long loaded, int filtered, int unselected)
            throws Exception {
        return response(
                "{\"TxnId\":9,\"Label\":\"label\",\"Status\":\"Success\","
                        + "\"NumberTotalRows\":"
                        + total
                        + ",\"NumberLoadedRows\":"
                        + loaded
                        + ",\"NumberFilteredRows\":"
                        + filtered
                        + ",\"NumberUnselectedRows\":"
                        + unselected
                        + "}");
    }

    private RespContent response(String json) throws Exception {
        return json == null ? null : OBJECT_MAPPER.readValue(json, RespContent.class);
    }
}
