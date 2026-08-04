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
import org.apache.doris.flink.rest.models.RespContent;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class BatchFlushResultTest {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Test
    public void snapshotsLoadResponsesAndExposesImmutableResults() throws Exception {
        RespContent response =
                OBJECT_MAPPER.readValue(
                        "{\"TxnId\":7,\"Label\":\"label-1\",\"Status\":\"Success\","
                                + "\"Message\":\"OK\",\"NumberTotalRows\":2,"
                                + "\"NumberLoadedRows\":2,\"NumberFilteredRows\":0,"
                                + "\"NumberUnselectedRows\":0,\"LoadBytes\":12}",
                        RespContent.class);
        BatchLoadResult load =
                BatchLoadResult.completed(1L, 1L, "db", "tbl", 2L, 12L, "VISIBLE", response);
        List<BatchLoadResult> mutableLoads = new ArrayList<>();
        mutableLoads.add(load);

        BatchFlushResult result = BatchFlushResult.completed(0L, 1L, mutableLoads);
        mutableLoads.clear();
        response.setMessage("mutated");

        Assert.assertEquals(0L, result.getFromSequenceExclusive());
        Assert.assertEquals(1L, result.getThroughSequenceInclusive());
        Assert.assertEquals(1, result.getLoadResults().size());
        Assert.assertEquals(2L, result.getSubmittedRows());
        Assert.assertEquals(2L, result.getTotalRows());
        Assert.assertEquals(2L, result.getLoadedRows());
        Assert.assertEquals(0L, result.getFilteredRows());
        Assert.assertEquals(0L, result.getUnselectedRows());
        Assert.assertEquals("OK", result.getLoadResults().get(0).getMessage());
        Assert.assertEquals(Long.valueOf(2L), result.getLoadResults().get(0).getLoadedRows());
        Assert.assertEquals(2L, result.getLoadResults().get(0).getSubmittedRows());

        try {
            result.getLoadResults().clear();
            Assert.fail("load results must be immutable");
        } catch (UnsupportedOperationException expected) {
            // expected
        }
    }

    @Test
    public void emptyResultHasAnExplicitStableEpoch() {
        BatchFlushResult result = BatchFlushResult.empty(3L);

        Assert.assertTrue(result.isEmpty());
        Assert.assertEquals(3L, result.getFromSequenceExclusive());
        Assert.assertEquals(3L, result.getThroughSequenceInclusive());
    }
}
