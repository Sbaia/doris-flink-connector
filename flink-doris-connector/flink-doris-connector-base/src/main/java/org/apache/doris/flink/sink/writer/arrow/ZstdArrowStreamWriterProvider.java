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

import org.apache.flink.api.python.shaded.org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.api.python.shaded.org.apache.arrow.vector.ipc.ArrowStreamWriter;

import java.nio.channels.WritableByteChannel;
import java.util.Iterator;
import java.util.ServiceLoader;

/** Flink-profile bridge for creating an Arrow IPC writer with ZSTD compression. */
public interface ZstdArrowStreamWriterProvider {

    ArrowStreamWriter create(VectorSchemaRoot root, WritableByteChannel channel);

    static ZstdArrowStreamWriterProvider load() {
        ServiceLoader<ZstdArrowStreamWriterProvider> providers =
                ServiceLoader.load(
                        ZstdArrowStreamWriterProvider.class,
                        ZstdArrowStreamWriterProvider.class.getClassLoader());
        Iterator<ZstdArrowStreamWriterProvider> iterator = providers.iterator();
        if (!iterator.hasNext()) {
            throw new IllegalStateException(
                    "Arrow ZSTD compression is unavailable for this Flink profile");
        }
        ZstdArrowStreamWriterProvider provider = iterator.next();
        if (iterator.hasNext()) {
            throw new IllegalStateException(
                    "Multiple Arrow ZSTD compression providers are available for this Flink profile");
        }
        return provider;
    }
}
