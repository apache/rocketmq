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
package org.apache.rocketmq.tieredstore.common;

import java.nio.ByteBuffer;
import java.util.Map;
import org.apache.rocketmq.store.ConsumeQueueExt;
import org.apache.rocketmq.store.GetMessageResult;
import org.apache.rocketmq.store.GetMessageStatus;
import org.apache.rocketmq.store.MessageFilter;
import org.apache.rocketmq.store.SelectMappedBufferResult;
import org.apache.rocketmq.tieredstore.util.MessageFormatUtilTest;
import org.junit.Assert;
import org.junit.Test;

public class GetMessageResultExtTest {

    @Test
    public void doFilterTest() {
        GetMessageResultExt resultExt = new GetMessageResultExt();
        Assert.assertNull(resultExt.getStatus());
        Assert.assertEquals(0, resultExt.doFilterMessage(null).getMessageCount());

        resultExt.setStatus(GetMessageStatus.OFFSET_OVERFLOW_ONE);
        Assert.assertEquals(0, resultExt.doFilterMessage(null).getMessageCount());

        resultExt.setStatus(GetMessageStatus.OFFSET_OVERFLOW_BADLY);
        Assert.assertEquals(0, resultExt.doFilterMessage(null).getMessageCount());

        int total = 3;
        for (int i = 0; i < total; i++) {
            resultExt.addMessageExt(new SelectMappedBufferResult(i * 1000L,
                    MessageFormatUtilTest.buildMockedMessageBuffer(), 1000, null),
                0, ("Tag" + i).hashCode());
        }
        Assert.assertEquals(total, resultExt.getMessageCount());
        Assert.assertEquals(total, resultExt.getTagCodeList().size());

        resultExt.setStatus(GetMessageStatus.FOUND);
        GetMessageResult getMessageResult = resultExt.doFilterMessage(new MessageFilter() {
            @Override
            public boolean isMatchedByConsumeQueue(Long tagsCode, ConsumeQueueExt.CqExtUnit cqExtUnit) {
                return false;
            }

            @Override
            public boolean isMatchedByCommitLog(ByteBuffer msgBuffer, Map<String, String> properties) {
                return false;
            }
        });
        Assert.assertEquals(0, getMessageResult.getMessageCount());

        getMessageResult = resultExt.doFilterMessage(new MessageFilter() {
            @Override
            public boolean isMatchedByConsumeQueue(Long tagsCode, ConsumeQueueExt.CqExtUnit cqExtUnit) {
                return "Tag1".hashCode() == tagsCode;
            }

            @Override
            public boolean isMatchedByCommitLog(ByteBuffer msgBuffer, Map<String, String> properties) {
                return false;
            }
        });
        Assert.assertEquals(0, getMessageResult.getMessageCount());

        getMessageResult = resultExt.doFilterMessage(new MessageFilter() {
            @Override
            public boolean isMatchedByConsumeQueue(Long tagsCode, ConsumeQueueExt.CqExtUnit cqExtUnit) {
                return "Tag1".hashCode() == tagsCode;
            }

            @Override
            public boolean isMatchedByCommitLog(ByteBuffer msgBuffer, Map<String, String> properties) {
                return true;
            }
        });
        Assert.assertEquals(1, getMessageResult.getMessageCount());
    }
}