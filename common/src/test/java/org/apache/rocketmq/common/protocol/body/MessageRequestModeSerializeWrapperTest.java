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

import org.apache.rocketmq.common.message.MessageRequestMode;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;
import org.junit.Test;

import java.util.concurrent.ConcurrentHashMap;

import static org.assertj.core.api.Assertions.assertThat;

public class MessageRequestModeSerializeWrapperTest {

    @Test
    public void testFromJson(){
        MessageRequestModeSerializeWrapper  messageRequestModeSerializeWrapper = new MessageRequestModeSerializeWrapper();
        ConcurrentHashMap<String, ConcurrentHashMap<String, SetMessageRequestModeRequestBody>>
                messageRequestModeMap = new ConcurrentHashMap<String, ConcurrentHashMap<String, SetMessageRequestModeRequestBody>>();
        String topic = "TopicTest";
        String group = "Consumer";
        MessageRequestMode requestMode = MessageRequestMode.POP;
        int popShareQueueNum = -1;
        SetMessageRequestModeRequestBody requestBody = new SetMessageRequestModeRequestBody();
        requestBody.setTopic(topic);
        requestBody.setConsumerGroup(group);
        requestBody.setMode(requestMode);
        requestBody.setPopShareQueueNum(popShareQueueNum);
        ConcurrentHashMap<String, SetMessageRequestModeRequestBody> map = new ConcurrentHashMap<>();
        map.put(group, requestBody);
        messageRequestModeMap.put(topic, map);

        messageRequestModeSerializeWrapper.setMessageRequestModeMap(messageRequestModeMap);

        String json = RemotingSerializable.toJson(messageRequestModeSerializeWrapper, true);
        MessageRequestModeSerializeWrapper fromJson = RemotingSerializable.fromJson(json, MessageRequestModeSerializeWrapper.class);
        assertThat(fromJson.getMessageRequestModeMap()).containsKey(topic);
        assertThat(fromJson.getMessageRequestModeMap().get(topic)).containsKey(group);
        assertThat(fromJson.getMessageRequestModeMap().get(topic).get(group).getTopic()).isEqualTo(topic);
        assertThat(fromJson.getMessageRequestModeMap().get(topic).get(group).getConsumerGroup()).isEqualTo(group);
        assertThat(fromJson.getMessageRequestModeMap().get(topic).get(group).getMode()).isEqualTo(requestMode);
        assertThat(fromJson.getMessageRequestModeMap().get(topic).get(group).getPopShareQueueNum()).isEqualTo(popShareQueueNum);
    }
}
