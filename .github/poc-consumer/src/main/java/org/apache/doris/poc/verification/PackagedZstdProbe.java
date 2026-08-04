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
package org.apache.doris.poc.verification;

import com.github.luben.zstd.Zstd;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/** Runtime smoke test for the JNI-backed ZSTD classes embedded in the shaded connector jar. */
public final class PackagedZstdProbe {

    private PackagedZstdProbe() {
    }

    public static void main(String[] args) {
        byte[] source = "observable-flush-zstd-native-probe"
                .getBytes(StandardCharsets.UTF_8);
        byte[] compressed = Zstd.compress(source);
        byte[] restored = Zstd.decompress(compressed, source.length);
        if (!Arrays.equals(source, restored)) {
            throw new IllegalStateException("Packaged zstd-jni round trip changed the payload");
        }
    }
}
