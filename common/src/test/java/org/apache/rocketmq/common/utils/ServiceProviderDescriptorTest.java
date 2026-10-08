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

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ServiceProviderDescriptorTest {
    public interface TestService {
    }

    public static class FirstService implements TestService {
    }

    public static class SecondService implements TestService {
    }

    @Test
    public void testLoadSkipsBlankLinesAndCommentsAndDuplicates() {
        String descriptor = "# providers\n\n  " + FirstService.class.getName() + " # first\n"
            + "\n" + SecondService.class.getName() + "\n" + FirstService.class.getName() + "\n";
        withDescriptor(descriptor, () -> {
            List<TestService> services = ServiceProvider.load("test/providers", TestService.class);
            assertEquals(2, services.size());
            assertTrue(services.get(0) instanceof FirstService);
            assertTrue(services.get(1) instanceof SecondService);
        });
    }

    @Test
    public void testLoadClassSkipsLeadingBlankLinesAndComments() {
        withDescriptor("\n# comment\n  " + SecondService.class.getName() + " # selected\n", () -> {
            TestService service = ServiceProvider.loadClass("test/providers", TestService.class);
            assertTrue(service instanceof SecondService);
        });
    }

    private void withDescriptor(String descriptor, Runnable assertion) {
        ClassLoader previous = Thread.currentThread().getContextClassLoader();
        ClassLoader loader = new ClassLoader(previous) {
            @Override
            public InputStream getResourceAsStream(String name) {
                if ("test/providers".equals(name)) {
                    return new ByteArrayInputStream(descriptor.getBytes(StandardCharsets.UTF_8));
                }
                return super.getResourceAsStream(name);
            }
        };
        Thread.currentThread().setContextClassLoader(loader);
        try {
            assertion.run();
        } finally {
            Thread.currentThread().setContextClassLoader(previous);
        }
    }
}
