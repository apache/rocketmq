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

package org.apache.rocketmq.common.protocol;

import org.apache.rocketmq.common.message.MessageQueueForC;
import org.apache.rocketmq.common.protocol.body.ResetOffsetBodyForC;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @date: 2019-02-26 09:37
 */
public class ResetOffsetBodyForCTest {
    @Test
    public void testFromJson() {
        int assertSize = 1 ,index = 0;
        String topic = "topic",brokerName = "broker";
        int queueId = 1;
        long offset = 2l;
        ResetOffsetBodyForC robc = new ResetOffsetBodyForC();
        List<MessageQueueForC> offsetTable = new ArrayList<MessageQueueForC>();
        MessageQueueForC messageQueueForC = new MessageQueueForC(topic,brokerName,queueId,offset);
        offsetTable.add(messageQueueForC);
        robc.setOffsetTable(offsetTable);
        String json = RemotingSerializable.toJson(robc, true);
        ResetOffsetBodyForC fromJson = RemotingSerializable.fromJson(json, ResetOffsetBodyForC.class);
        assertThat(fromJson.getOffsetTable().get(index)).isNotNull();
        assertThat(fromJson.getOffsetTable().size()).isEqualTo(assertSize);
        assertThat(fromJson.getOffsetTable().get(index).getBrokerName()).isEqualTo(brokerName);
        assertThat(fromJson.getOffsetTable().get(index).getTopic()).isEqualTo(topic);
        assertThat(fromJson.getOffsetTable().get(index).getQueueId()).isEqualTo(queueId);
        assertThat(fromJson.getOffsetTable().get(index).getOffset()).isEqualTo(offset);
    }
}