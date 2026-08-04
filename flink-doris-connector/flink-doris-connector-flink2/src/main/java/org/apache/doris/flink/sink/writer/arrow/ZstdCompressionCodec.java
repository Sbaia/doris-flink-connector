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

import org.apache.flink.api.python.shaded.org.apache.arrow.memory.ArrowBuf;
import org.apache.flink.api.python.shaded.org.apache.arrow.memory.BufferAllocator;
import org.apache.flink.api.python.shaded.org.apache.arrow.vector.compression.AbstractCompressionCodec;
import org.apache.flink.api.python.shaded.org.apache.arrow.vector.compression.CompressionCodec;
import org.apache.flink.api.python.shaded.org.apache.arrow.vector.compression.CompressionUtil;

import com.github.luben.zstd.Zstd;

/** ZSTD codec for the Arrow IPC implementation bundled with Flink 2. */
final class ZstdCompressionCodec extends AbstractCompressionCodec {

    static void verifyNativeLibrary() {
        try {
            Zstd.defaultCompressionLevel();
        } catch (LinkageError failure) {
            throw new IllegalStateException(
                    "Arrow ZSTD requires a loadable, non-relocated zstd-jni native library",
                    failure);
        }
    }

    @Override
    protected ArrowBuf doCompress(BufferAllocator allocator, ArrowBuf uncompressedBuffer) {
        long uncompressedLength = uncompressedBuffer.writerIndex();
        byte[] uncompressedBytes = new byte[(int) uncompressedLength];
        uncompressedBuffer.getBytes(0, uncompressedBytes);
        byte[] compressedBytes = Zstd.compress(uncompressedBytes);

        long outputSize = 8L + compressedBytes.length;
        ArrowBuf compressedBuffer = allocator.buffer(outputSize);
        compressedBuffer.setBytes(8, compressedBytes);
        compressedBuffer.writerIndex(outputSize);
        return compressedBuffer;
    }

    @Override
    protected ArrowBuf doDecompress(BufferAllocator allocator, ArrowBuf compressedBuffer) {
        long uncompressedLength = readUncompressedLength(compressedBuffer);
        int compressedLength = (int) (compressedBuffer.writerIndex() - 8);
        byte[] compressedBytes = new byte[compressedLength];
        compressedBuffer.getBytes(8, compressedBytes);
        byte[] decompressedBytes = Zstd.decompress(compressedBytes, (int) uncompressedLength);

        ArrowBuf decompressedBuffer = allocator.buffer(decompressedBytes.length);
        decompressedBuffer.setBytes(0, decompressedBytes);
        decompressedBuffer.writerIndex(decompressedBytes.length);
        return decompressedBuffer;
    }

    @Override
    public CompressionUtil.CodecType getCodecType() {
        return CompressionUtil.CodecType.ZSTD;
    }

    /** Factory for creating ZSTD codecs. */
    static class Factory implements CompressionCodec.Factory {
        @Override
        public CompressionCodec createCodec(CompressionUtil.CodecType codecType) {
            if (codecType != CompressionUtil.CodecType.ZSTD) {
                throw new IllegalArgumentException("Only ZSTD is supported, got: " + codecType);
            }
            return new ZstdCompressionCodec();
        }

        @Override
        public CompressionCodec createCodec(
                CompressionUtil.CodecType codecType, int compressionLevel) {
            return createCodec(codecType);
        }
    }
}
