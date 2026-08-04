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

import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class TableLoadDispatcherTest {

    @Test
    public void differentTablesRunConcurrentlyOnABoundedWorkerPool() throws Exception {
        AtomicReference<Throwable> failure = new AtomicReference<>();
        TableLoadDispatcher dispatcher = new TableLoadDispatcher(2, failure);
        CountDownLatch firstTwoStarted = new CountDownLatch(2);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger inFlight = new AtomicInteger();
        AtomicInteger maxInFlight = new AtomicInteger();
        Set<String> workerThreads = Collections.synchronizedSet(new HashSet<String>());

        try {
            for (int table = 0; table < 2; table++) {
                dispatcher.submit(
                        "db.table_" + table,
                        () -> {
                            workerThreads.add(Thread.currentThread().getName());
                            int current = inFlight.incrementAndGet();
                            maxInFlight.accumulateAndGet(current, Math::max);
                            firstTwoStarted.countDown();
                            try {
                                if (!release.await(5, TimeUnit.SECONDS)) {
                                    throw new IllegalStateException("test did not release workers");
                                }
                            } finally {
                                inFlight.decrementAndGet();
                            }
                        });
            }

            Assert.assertTrue(
                    "different tables did not overlap", firstTwoStarted.await(5, TimeUnit.SECONDS));
            Assert.assertEquals(2, maxInFlight.get());
            release.countDown();
            for (int table = 2; table < 20; table++) {
                dispatcher.submit(
                        "db.table_" + table,
                        () -> workerThreads.add(Thread.currentThread().getName()));
            }
            Assert.assertTrue(dispatcher.awaitIdle(5, TimeUnit.SECONDS));
            Assert.assertTrue("worker count exceeded configured bound", workerThreads.size() <= 2);
            Assert.assertNull(failure.get());
        } finally {
            release.countDown();
            dispatcher.close();
        }
    }

    @Test
    public void sameTablePreservesSubmissionOrder() throws Exception {
        AtomicReference<Throwable> failure = new AtomicReference<>();
        TableLoadDispatcher dispatcher = new TableLoadDispatcher(2, failure);
        CountDownLatch firstStarted = new CountDownLatch(1);
        CountDownLatch releaseFirst = new CountDownLatch(1);
        AtomicBoolean secondStarted = new AtomicBoolean(false);

        try {
            dispatcher.submit(
                    "db.table",
                    () -> {
                        firstStarted.countDown();
                        if (!releaseFirst.await(5, TimeUnit.SECONDS)) {
                            throw new IllegalStateException("test did not release first load");
                        }
                    });
            dispatcher.submit("db.table", () -> secondStarted.set(true));

            Assert.assertTrue(firstStarted.await(5, TimeUnit.SECONDS));
            Thread.sleep(100L);
            Assert.assertFalse("second load overtook the first", secondStarted.get());
            releaseFirst.countDown();
            Assert.assertTrue(dispatcher.awaitIdle(5, TimeUnit.SECONDS));
            Assert.assertTrue(secondStarted.get());
            Assert.assertNull(failure.get());
        } finally {
            releaseFirst.countDown();
            dispatcher.close();
        }
    }

    @Test
    public void firstLoadFailureWinsAndLaterWorkIsSkipped() throws Exception {
        AtomicReference<Throwable> failure = new AtomicReference<>();
        TableLoadDispatcher dispatcher = new TableLoadDispatcher(1, failure);
        AtomicBoolean laterWorkRan = new AtomicBoolean(false);

        try {
            dispatcher.submit(
                    "db.first",
                    () -> {
                        throw new IllegalStateException("first failure");
                    });
            dispatcher.submit("db.second", () -> laterWorkRan.set(true));

            Assert.assertTrue(dispatcher.awaitIdle(5, TimeUnit.SECONDS));
            Assert.assertNotNull(failure.get());
            Assert.assertEquals("first failure", failure.get().getMessage());
            Assert.assertFalse("work continued after the terminal failure", laterWorkRan.get());
        } finally {
            dispatcher.close();
        }
    }

    @Test(expected = IllegalStateException.class)
    public void closedDispatcherRejectsNewLoads() throws Exception {
        TableLoadDispatcher dispatcher =
                new TableLoadDispatcher(1, new AtomicReference<Throwable>());
        dispatcher.close();
        dispatcher.submit("db.table", () -> {});
    }
}
