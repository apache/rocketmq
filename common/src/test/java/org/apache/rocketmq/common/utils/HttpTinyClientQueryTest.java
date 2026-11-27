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

package org.apache.rocketmq.common.utils;

import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class HttpTinyClientQueryTest {
    @Test
    public void testAppendEncodedParametersToExistingQuery() throws Exception {
        assertQuery("?fixed=1", Arrays.asList("user name", "A+B \u4e2d"),
            "fixed=1&user+name=A%2BB+%E4%B8%AD");
    }

    @Test
    public void testAppendParametersBeforeFragment() throws Exception {
        assertQuery("?fixed=1#fragment", Arrays.asList("key", "value"), "fixed=1&key=value");
    }

    @Test
    public void testEmptyParametersKeepExistingQuery() throws Exception {
        assertQuery("?fixed=1", Collections.emptyList(), "fixed=1");
    }

    private void assertQuery(String suffix, java.util.List<String> parameters, String expected) throws Exception {
        AtomicReference<String> query = new AtomicReference<>();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/query", exchange -> {
            query.set(exchange.getRequestURI().getRawQuery());
            byte[] response = "OK".getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, response.length);
            exchange.getResponseBody().write(response);
            exchange.close();
        });
        server.start();
        try {
            HttpTinyClient.HttpResult result = HttpTinyClient.httpGet(
                "http://127.0.0.1:" + server.getAddress().getPort() + "/query" + suffix,
                null, parameters, "UTF-8", 3000);
            assertEquals(200, result.code);
            assertEquals("OK", result.content);
            assertEquals(expected, query.get());
        } finally {
            server.stop(0);
        }
    }
}
