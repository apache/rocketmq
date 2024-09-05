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
package org.apache.rocketmq.common.coldctr;

import java.util.concurrent.atomic.AtomicLong;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AccAndTimeStampTest {

    private AccAndTimeStamp accAndTimeStamp;

    @Before
    public void setUp() {
        accAndTimeStamp = new AccAndTimeStamp(new AtomicLong());
    }

    @Test
    public void testInitialValues() {
        assertEquals("Cold accumulator should be initialized to 0", 0, accAndTimeStamp.getColdAcc().get());
        assertTrue("Last cold read time should be initialized to current time", accAndTimeStamp.getLastColdReadTimeMills() >= System.currentTimeMillis() - 1000);
        assertTrue("Create time should be initialized to current time", accAndTimeStamp.getCreateTimeMills() >= System.currentTimeMillis() - 1000);
    }

    @Test
    public void testSetColdAcc() {
        AtomicLong newColdAcc = new AtomicLong(100L);
        accAndTimeStamp.setColdAcc(newColdAcc);
        assertEquals("Cold accumulator should be set to new value", newColdAcc, accAndTimeStamp.getColdAcc());
    }

    @Test
    public void testSetLastColdReadTimeMills() {
        long newLastColdReadTimeMills = System.currentTimeMillis() + 1000;
        accAndTimeStamp.setLastColdReadTimeMills(newLastColdReadTimeMills);
        assertEquals("Last cold read time should be set to new value", newLastColdReadTimeMills, accAndTimeStamp.getLastColdReadTimeMills().longValue());
    }

    @Test
    public void testSetCreateTimeMills() {
        long newCreateTimeMills = System.currentTimeMillis() + 2000;
        accAndTimeStamp.setCreateTimeMills(newCreateTimeMills);
        assertEquals("Create time should be set to new value", newCreateTimeMills, accAndTimeStamp.getCreateTimeMills().longValue());
    }

    @Test
    public void testToStringContainsCorrectInformation() {
        String toStringOutput = accAndTimeStamp.toString();
        assertTrue("ToString should contain cold accumulator value", toStringOutput.contains("coldAcc=" + accAndTimeStamp.getColdAcc()));
        assertTrue("ToString should contain last cold read time", toStringOutput.contains("lastColdReadTimeMills=" + accAndTimeStamp.getLastColdReadTimeMills()));
        assertTrue("ToString should contain create time", toStringOutput.contains("createTimeMills=" + accAndTimeStamp.getCreateTimeMills()));
    }
}
