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
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.rocketmq.common;

import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.StringContains.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * CountDownLatch2 Unit Test
 * @see CountDownLatch2
 *
 * @since 2019-02-21
 */
public class CountDownLatch2Test {

    /**
     * test constructor with invalid init param
     */
    @Test
    public void testConstructorError() {
        int count = -1;
        try {
            CountDownLatch2 latch = new CountDownLatch2(count);
            fail("Expected an IllegalArgumentException to be thrown");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), is("count < 0"));
        }
    }

    /**
     * test constructor with valid init param
     */
    @Test
    public void testConstructor() {
        int count = 10;
        CountDownLatch2 latch = new CountDownLatch2(count);
        assertEquals("Expected equal", count, latch.getCount());
        assertThat("Expected contain", latch.toString(), containsString("[Count = " + count + "]"));
    }

    @Test(timeout = 2000)
    public void testCore() throws InterruptedException {
        int count = 2;
        CountDownLatch2 latch = new CountDownLatch2(count);
        // test #countDown
        latch.countDown();
        assertEquals("Expected equal", count - 1, latch.getCount());
        // test #reset
        latch.reset();
        assertEquals("Expected equal", count, latch.getCount());
        //test #await timeout
        latch.await(1, TimeUnit.SECONDS);
        assertEquals("Expected equal", count, latch.getCount());
        //test #await
        latch.countDown();
        latch.countDown();
        latch.await();
        assertEquals("Expected equal", 0, latch.getCount());
        // coverage Sync#tryReleaseShared, c==0
        latch.countDown();
        assertEquals("Expected equal", 0, latch.getCount());
    }
}
