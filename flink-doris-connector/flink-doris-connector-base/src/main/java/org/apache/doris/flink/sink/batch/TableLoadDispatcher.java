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

import org.apache.flink.util.Preconditions;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/** Dispatches loads on a bounded pool while preserving FIFO ordering for each table. */
final class TableLoadDispatcher implements AutoCloseable {

    @FunctionalInterface
    interface LoadOperation {
        void run() throws Exception;
    }

    private final Object monitor = new Object();
    private final ExecutorService workers;
    private final AtomicReference<Throwable> terminalFailure;
    private final Semaphore pendingCapacity;
    private final Map<String, CompletableFuture<Void>> tableTails = new HashMap<>();
    private final AtomicBoolean closed = new AtomicBoolean(false);

    TableLoadDispatcher(int concurrency, AtomicReference<Throwable> terminalFailure) {
        Preconditions.checkArgument(concurrency > 0, "concurrency must be positive");
        this.terminalFailure = Preconditions.checkNotNull(terminalFailure);
        this.pendingCapacity = new Semaphore(concurrency);
        this.workers =
                Executors.newFixedThreadPool(
                        concurrency,
                        new ThreadFactory() {
                            private final AtomicInteger threadNumber = new AtomicInteger();

                            @Override
                            public Thread newThread(Runnable runnable) {
                                Thread thread =
                                        new Thread(
                                                runnable,
                                                "doris-table-load-"
                                                        + threadNumber.incrementAndGet());
                                thread.setDaemon(true);
                                return thread;
                            }
                        });
    }

    void submit(String tableIdentifier, LoadOperation operation) throws InterruptedException {
        Preconditions.checkNotNull(tableIdentifier, "tableIdentifier must not be null");
        Preconditions.checkNotNull(operation, "operation must not be null");
        pendingCapacity.acquire();
        synchronized (monitor) {
            if (closed.get()) {
                pendingCapacity.release();
                throw new IllegalStateException("Table load dispatcher is closed");
            }
            try {
                CompletableFuture<Void> previous = tableTails.get(tableIdentifier);
                CompletableFuture<Void> next;
                if (previous == null) {
                    next = CompletableFuture.runAsync(() -> execute(operation), workers);
                } else {
                    next =
                            previous.handle((ignored, ignoredFailure) -> null)
                                    .thenRunAsync(() -> execute(operation), workers);
                }
                tableTails.put(tableIdentifier, next);
                next.whenComplete(
                        (ignored, ignoredFailure) -> {
                            pendingCapacity.release();
                            synchronized (monitor) {
                                if (tableTails.get(tableIdentifier) == next) {
                                    tableTails.remove(tableIdentifier);
                                }
                                monitor.notifyAll();
                            }
                        });
            } catch (RuntimeException failure) {
                pendingCapacity.release();
                throw failure;
            }
        }
    }

    boolean awaitIdle(long timeout, TimeUnit unit) throws InterruptedException {
        long remainingNanos = unit.toNanos(timeout);
        long deadline = System.nanoTime() + remainingNanos;
        synchronized (monitor) {
            while (!tableTails.isEmpty() && remainingNanos > 0L) {
                TimeUnit.NANOSECONDS.timedWait(monitor, remainingNanos);
                remainingNanos = deadline - System.nanoTime();
            }
            return tableTails.isEmpty();
        }
    }

    private void execute(LoadOperation operation) {
        if (terminalFailure.get() != null || closed.get()) {
            return;
        }
        try {
            operation.run();
        } catch (Throwable failure) {
            terminalFailure.compareAndSet(null, failure);
        }
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            workers.shutdownNow();
        }
        synchronized (monitor) {
            monitor.notifyAll();
        }
    }
}
