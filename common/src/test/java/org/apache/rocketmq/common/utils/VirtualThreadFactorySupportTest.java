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

import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.ThreadFactory;

public class VirtualThreadFactorySupportTest {
    @Test
    public void testVirtualThreadEnabled() {
        final ThreadFactory threadFactory = VirtualThreadFactorySupport.create("test");
        if (getMajorJavaVersion() >= 21) {
            Assert.assertNotNull(threadFactory);
            final Thread thread = threadFactory.newThread(() -> {
                //NOOP
            });
            Assert.assertNotNull(thread);
            Assert.assertEquals("VirtualThread", thread.getClass().getSimpleName());
        } else {
            Assert.assertNull(threadFactory);
        }
    }

    private static int getMajorJavaVersion() {
        final String version = System.getProperty("java.specification.version");
        if (version.indexOf('.') > 0) {
            try {
                final String[] parts = version.split("[._]");
                int first = Integer.parseInt(parts[0]);
                if (first == 1 && parts.length > 1) {
                    return Integer.parseInt(parts[1]);
                } else {
                    return first;
                }
            } catch (Exception e) {
                return -1;
            }
        }
        return Integer.parseInt(version);
    }
}
