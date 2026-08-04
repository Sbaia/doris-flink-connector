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

package org.apache.doris.flink.sink;

import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.HttpClientBuilder;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;

public class HttpUtilTest {

    @Test
    public void batchClientsUseConfiguredReadTimeout() throws Exception {
        DorisReadOptions readOptions =
                DorisReadOptions.builder()
                        .setRequestConnectTimeoutMs(1_234)
                        .setRequestReadTimeoutMs(2_345)
                        .build();
        HttpUtil httpUtil = new HttpUtil(readOptions, false);

        Assert.assertEquals(
                2_345, requestConfig(httpUtil.getHttpClientBuilderForBatch()).getSocketTimeout());
        Assert.assertEquals(
                2_345,
                requestConfig(httpUtil.getHttpClientBuilderForCopyBatch()).getSocketTimeout());
    }

    private RequestConfig requestConfig(HttpClientBuilder builder) throws Exception {
        Field field = HttpClientBuilder.class.getDeclaredField("defaultRequestConfig");
        field.setAccessible(true);
        return (RequestConfig) field.get(builder);
    }
}
