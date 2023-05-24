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
package org.apache.rocketmq.store;

import org.junit.Assert;
import org.junit.Test;

public class GetMessageResultTest {

    @Test
    public void testAddMessage() {
        GetMessageResult getMessageResult = new GetMessageResult();
        SelectMappedBufferResult mappedBufferResult1 = new SelectMappedBufferResult(0, null, 4 * 1024, null);
        getMessageResult.addMessage(mappedBufferResult1);

        SelectMappedBufferResult mappedBufferResult2 = new SelectMappedBufferResult(0, null, 2 * 4 * 1024, null);
        getMessageResult.addMessage(mappedBufferResult2, 0);

        SelectMappedBufferResult mappedBufferResult3 = new SelectMappedBufferResult(0, null, 4 * 4 * 1024, null);
        getMessageResult.addMessage(mappedBufferResult3, 0, 2);

        Assert.assertEquals(getMessageResult.getMessageQueueOffset().size(), 2);
        Assert.assertEquals(getMessageResult.getMessageBufferList().size(), 3);
        Assert.assertEquals(getMessageResult.getMessageMapedList().size(), 3);
        Assert.assertEquals(getMessageResult.getMessageCount(), 4);
        Assert.assertEquals(getMessageResult.getMsgCount4Commercial(), 1 + 2 + 4);
        Assert.assertEquals(getMessageResult.getBufferTotalSize(), (1 + 2 + 4) * 4 * 1024);
    }
}
