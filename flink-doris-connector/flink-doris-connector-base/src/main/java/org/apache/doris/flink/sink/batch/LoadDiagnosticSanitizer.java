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

import java.net.URI;
import java.net.URISyntaxException;

final class LoadDiagnosticSanitizer {
    private static final int MAX_DIAGNOSTIC_LENGTH = 512;

    private LoadDiagnosticSanitizer() {}

    static String text(String value) {
        if (value == null) {
            return null;
        }
        StringBuilder sanitized =
                new StringBuilder(Math.min(value.length(), MAX_DIAGNOSTIC_LENGTH));
        boolean previousWhitespace = false;
        for (int index = 0;
                index < value.length() && sanitized.length() < MAX_DIAGNOSTIC_LENGTH;
                index++) {
            char character = value.charAt(index);
            boolean whitespace =
                    Character.isWhitespace(character) || Character.isISOControl(character);
            if (whitespace) {
                if (!previousWhitespace && sanitized.length() > 0) {
                    sanitized.append(' ');
                }
            } else {
                sanitized.append(character);
            }
            previousWhitespace = whitespace;
        }
        return sanitized.toString().trim();
    }

    static String url(String value) {
        if (value == null || value.trim().isEmpty()) {
            return null;
        }
        try {
            URI original = new URI(value.trim());
            if (original.getScheme() == null || original.getHost() == null) {
                return "<redacted-invalid-url>";
            }
            URI sanitized =
                    new URI(
                            original.getScheme(),
                            null,
                            original.getHost(),
                            original.getPort(),
                            original.getPath(),
                            null,
                            null);
            return text(sanitized.toASCIIString());
        } catch (URISyntaxException ignored) {
            return "<redacted-invalid-url>";
        }
    }
}
