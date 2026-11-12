/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.common;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLStreamHandler;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MixAllUrlReadTest {
    @Test
    public void testReadEntireUrlDespiteAvailableAndShortReads() throws Exception {
        String content = "\u914d\u7f6e\u6587\u4ef6: \u4e2d\u6587 and UTF-8 \ud83d\ude80";
        AtomicBoolean closed = new AtomicBoolean();
        InputStream stream = new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8)) {
            @Override
            public int available() {
                return 0;
            }

            @Override
            public synchronized int read(byte[] data, int offset, int length) {
                return super.read(data, offset, Math.min(length, 1));
            }

            @Override
            public void close() throws IOException {
                closed.set(true);
                super.close();
            }
        };
        URL url = new URL(null, "test://configuration", new URLStreamHandler() {
            @Override
            protected URLConnection openConnection(URL requestedUrl) {
                return new URLConnection(requestedUrl) {
                    @Override
                    public void connect() {
                    }

                    @Override
                    public InputStream getInputStream() {
                        return stream;
                    }
                };
            }
        });
        assertEquals(content, MixAll.file2String(url));
        assertTrue(closed.get());
    }
}
