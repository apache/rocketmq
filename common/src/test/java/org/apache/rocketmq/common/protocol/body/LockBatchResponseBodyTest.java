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

package org.apache.rocketmq.common.protocol.body;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;
import org.assertj.core.util.Lists;
import org.junit.Test;

public class LockBatchResponseBodyTest {

    @Test
    public void testFromJson() {
        MessageQueue messageQueue = new MessageQueue();
        messageQueue.setBrokerName("fakeBrokerName");
        messageQueue.setQueueId(1001);
        Set<MessageQueue> lockOKMQSet = new HashSet<MessageQueue>();
        lockOKMQSet.add(messageQueue);
        LockBatchResponseBody lockBatchResponseBody = new LockBatchResponseBody();
        lockBatchResponseBody.setLockOKMQSet(lockOKMQSet);

        String toJson = RemotingSerializable.toJson(lockBatchResponseBody, true);
        LockBatchResponseBody fromJson = RemotingSerializable.fromJson(toJson, LockBatchResponseBody.class);

        Set<MessageQueue> fromJsonLockOKMQSet = fromJson.getLockOKMQSet();
        assertThat(fromJsonLockOKMQSet).isInstanceOf(Set.class);

        List<MessageQueue> fromJsonLockOKMQList = Lists.newArrayList(fromJsonLockOKMQSet);
        MessageQueue fromJsonMessageQueue = fromJsonLockOKMQList.get(0);
        assertThat(fromJsonMessageQueue).isInstanceOf(MessageQueue.class);

        assertThat(fromJsonMessageQueue.getBrokerName()).isEqualTo(messageQueue.getBrokerName());
        assertThat(fromJsonMessageQueue.getQueueId()).isEqualTo(messageQueue.getQueueId());

    }
}
