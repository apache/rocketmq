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
package org.apache.rocketmq.common.message;

import java.util.Date;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.rocketmq.common.message.MessageConst.PROPERTY_TRACE_SWITCH;
import static org.apache.rocketmq.common.message.MessageConst.PROPERTY_DELAY_TIMESTAMP;
import static org.junit.Assert.*;

public class MessageTest {
    @Test(expected = RuntimeException.class)
    public void putUserPropertyWithRuntimeException() throws Exception {
        Message m = new Message();

        m.putUserProperty(PROPERTY_TRACE_SWITCH, "");
    }

    @Test(expected = IllegalArgumentException.class)
    public void putUserNullValuePropertyWithException() throws Exception {
        Message m = new Message();

        m.putUserProperty("prop1", null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void putUserEmptyValuePropertyWithException() throws Exception {
        Message m = new Message();

        m.putUserProperty("prop1", "   ");
    }

    @Test(expected = IllegalArgumentException.class)
    public void putUserNullNamePropertyWithException() throws Exception {
        Message m = new Message();

        m.putUserProperty(null, "val1");
    }

    @Test(expected = IllegalArgumentException.class)
    public void putUserEmptyNamePropertyWithException() throws Exception {
        Message m = new Message();

        m.putUserProperty("   ", "val1");
    }

    @Test
    public void putUserProperty() throws Exception {
        Message m = new Message();

        m.putUserProperty("prop1", "val1");
        Assert.assertEquals("val1", m.getUserProperty("prop1"));
    }

    @Test
    public void setDelaySecondTest() throws Exception {
        Message m = new Message();

        long delaySecond = ThreadLocalRandom.current().nextInt(1, 999);
        m.setDelaySecond(delaySecond);
        long delayTime = m.getDelayDate().getTime();

        long now = System.currentTimeMillis();
        long expect = now + (delaySecond * TimeUnit.SECONDS.toMillis(1));
        Assert.assertTrue(delayTime - expect <= 1);

        m.setDelaySecond(-1);
        Assert.assertNull(m.getDelayDate());
    }

    @Test
    public void setDelayDateTest() throws Exception {
        Message m = new Message();

        long now = System.currentTimeMillis();
        long expect = now + ThreadLocalRandom.current().nextInt(1, 9999999);
        Date delayDate = new Date(expect);
        m.setDelayDate(delayDate);

        long delayTime = m.getDelayDate().getTime();
        Assert.assertEquals(expect, delayTime);

        m.setDelayDate(null);
        Assert.assertNull(m.getDelayDate());
    }

    @Test(expected = RuntimeException.class)
    public void putPropertyDelayTimestampWithRuntimeException() throws Exception {
        Message m = new Message();

        m.putUserProperty(PROPERTY_DELAY_TIMESTAMP, "");
    }

}
