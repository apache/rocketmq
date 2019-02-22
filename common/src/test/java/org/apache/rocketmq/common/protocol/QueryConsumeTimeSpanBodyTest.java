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

import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.body.QueryConsumeTimeSpanBody;
import org.apache.rocketmq.common.protocol.body.QueueTimeSpan;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @Author dingzhiyong
 * @Date 2019/2/22
 */
public class QueryConsumeTimeSpanBodyTest {

    @Test
    public void testFromJson() throws Exception {
        QueryConsumeTimeSpanBody qctsb = new QueryConsumeTimeSpanBody();
        List<QueueTimeSpan> queueTimeSpans = new ArrayList<QueueTimeSpan>();
        QueueTimeSpan queueTimeSpan = new QueueTimeSpan();
        queueTimeSpan.setMinTimeStamp(1550825710000l);
        queueTimeSpan.setMaxTimeStamp(1550825790000l);
        queueTimeSpan.setConsumeTimeStamp(1550825760000l);
        queueTimeSpan.setDelayTime(5000l);
        MessageQueue messageQueue = new MessageQueue("topicName", "brokerName", 1);
        queueTimeSpan.setMessageQueue(messageQueue);
        queueTimeSpans.add(queueTimeSpan);
        qctsb.setConsumeTimeSpanSet(queueTimeSpans);
        String json = RemotingSerializable.toJson(qctsb, true);
        QueryConsumeTimeSpanBody fromJson = RemotingSerializable.fromJson(json, QueryConsumeTimeSpanBody.class);
        assertThat(fromJson.getConsumeTimeSpanSet().get(0).getMaxTimeStamp()).isEqualTo(1550825790000l);
        assertThat(fromJson.getConsumeTimeSpanSet().get(0).getMinTimeStamp()).isEqualTo(1550825710000l);
        assertThat(fromJson.getConsumeTimeSpanSet().get(0).getDelayTime()).isEqualTo(5000l);
        assertThat(fromJson.getConsumeTimeSpanSet().get(0).getMessageQueue()).isEqualTo(messageQueue);
    }
}
