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
package org.apache.rocketmq.client.impl.consumer;

import org.apache.rocketmq.remoting.protocol.body.PopProcessQueueInfo;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import static org.junit.Assert.assertEquals;

@RunWith(MockitoJUnitRunner.class)
public class PopProcessQueueTest {

    private final PopProcessQueueInfo popProcessQueueInfo = new PopProcessQueueInfo();

    @Test
    public void testPopProcessQueue() {
        long currentTime = System.currentTimeMillis();
        PopProcessQueue popRequest1 = createPopProcessQueue(currentTime);
        PopProcessQueue popRequest2 = createPopProcessQueue(currentTime);
        assertEquals(popRequest1.getLastPopTimestamp(), popRequest2.getLastPopTimestamp());
        assertEquals(popRequest1.toString(), popRequest2.toString());
        assertEquals(popRequest1.getWaiAckMsgCount(), popRequest2.getWaiAckMsgCount());
        assertEquals(popRequest1.ack(), popRequest2.ack());
        assertEquals(popRequest1.isPullExpired(), popRequest2.isPullExpired());
        assertEquals(popProcessQueueInfo.getLastPopTimestamp(), popRequest1.getLastPopTimestamp());
        assertEquals(popProcessQueueInfo.isDroped(), popRequest1.isDropped());
        assertEquals(popProcessQueueInfo.getWaitAckCount(), popRequest1.getWaiAckMsgCount() + popRequest2.getWaiAckMsgCount());
    }

    private PopProcessQueue createPopProcessQueue(final long currentTime) {
        PopProcessQueue result = new PopProcessQueue();
        long curTime = System.currentTimeMillis();
        result.setLastPopTimestamp(curTime);
        result.incFoundMsg(1);
        result.decFoundMsg(1);
        result.setLastPopTimestamp(currentTime);
        result.fillPopProcessQueueInfo(popProcessQueueInfo);
        return result;
    }
}
