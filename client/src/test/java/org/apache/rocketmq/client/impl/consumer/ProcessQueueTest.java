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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.protocol.body.ProcessQueueInfo;
import org.assertj.core.util.Lists;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(MockitoJUnitRunner.class)
public class ProcessQueueTest {

    @Test
    public void testCachedMessageCount() {
        ProcessQueue pq = new ProcessQueue();

        pq.putMessage(createMessageList());

        assertThat(pq.getMsgCount().get()).isEqualTo(100);

        pq.takeMessages(10);
        pq.commit();

        assertThat(pq.getMsgCount().get()).isEqualTo(90);

        pq.removeMessage(Collections.singletonList(pq.getMsgTreeMap().lastEntry().getValue()));
        assertThat(pq.getMsgCount().get()).isEqualTo(89);
    }

    @Test
    public void testCachedMessageSize() {
        ProcessQueue pq = new ProcessQueue();

        pq.putMessage(createMessageList());

        assertThat(pq.getMsgSize().get()).isEqualTo(100 * 123);

        pq.takeMessages(10);
        pq.commit();

        assertThat(pq.getMsgSize().get()).isEqualTo(90 * 123);

        pq.removeMessage(Collections.singletonList(pq.getMsgTreeMap().lastEntry().getValue()));
        assertThat(pq.getMsgSize().get()).isEqualTo(89 * 123);
    }

    @Test
    public void testContainsMessage() {
        ProcessQueue pq = new ProcessQueue();
        final List<MessageExt> messageList = createMessageList(2);
        final MessageExt message0 = messageList.get(0);
        final MessageExt message1 = messageList.get(1);

        pq.putMessage(Lists.list(message0));
        assertThat(pq.containsMessage(message0)).isTrue();
        assertThat(pq.containsMessage(message1)).isFalse();
    }

    @Test
    public void testFillProcessQueueInfo() {
        ProcessQueue pq = new ProcessQueue();
        pq.putMessage(createMessageList(102400));

        ProcessQueueInfo processQueueInfo = new ProcessQueueInfo();
        pq.fillProcessQueueInfo(processQueueInfo);

        assertThat(processQueueInfo.getCachedMsgSizeInMiB()).isEqualTo(12);

        pq.takeMessages(10000);
        pq.commit();
        pq.fillProcessQueueInfo(processQueueInfo);
        assertThat(processQueueInfo.getCachedMsgSizeInMiB()).isEqualTo(10);

        pq.takeMessages(10000);
        pq.commit();
        pq.fillProcessQueueInfo(processQueueInfo);
        assertThat(processQueueInfo.getCachedMsgSizeInMiB()).isEqualTo(9);

        pq.takeMessages(80000);
        pq.commit();
        pq.fillProcessQueueInfo(processQueueInfo);
        assertThat(processQueueInfo.getCachedMsgSizeInMiB()).isEqualTo(0);
    }

    private List<MessageExt> createMessageList() {
        return createMessageList(100);
    }

    private List<MessageExt> createMessageList(int count) {
        List<MessageExt> messageExtList = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            MessageExt messageExt = new MessageExt();
            messageExt.setQueueOffset(i);
            messageExt.setBody(new byte[123]);
            messageExtList.add(messageExt);
        }
        return messageExtList;
    }
}
