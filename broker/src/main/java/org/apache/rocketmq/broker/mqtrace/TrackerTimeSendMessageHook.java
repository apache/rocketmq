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

package org.apache.rocketmq.broker.mqtrace;

import java.util.Map;
import org.apache.rocketmq.broker.ServerTracerTimeUtil;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;

public class TrackerTimeSendMessageHook implements SendMessageHook {

    @Override
    public String hookName() {
        return "TrackerTimeSendMessageHook";
    }

    @Override
    public void sendMessageBefore(SendMessageContext context) {
        // brokerController.getBrokerConfig().isEnableTracerTime()
        String props = context.getMsgProps();
        if (props != null && props.length() > 1) {
            Map<String, String> properties = MessageDecoder.string2messageProperties(props);
            String messageTracerTimeId = properties.get(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX);
            if (properties.containsKey(MessageConst.MESSAGE_CREATE_TIME)) {
                ServerTracerTimeUtil.addMessageCreateTime(messageTracerTimeId, properties.get(MessageConst.MESSAGE_CREATE_TIME));
                ServerTracerTimeUtil.addMessageSendTime(messageTracerTimeId, properties.get(MessageConst.MESSAGE_SEND_TIME));
                ServerTracerTimeUtil.addMessageArriveBrokerTime(messageTracerTimeId, System.currentTimeMillis());
                ServerTracerTimeUtil.addMessageBeginSaveTime(messageTracerTimeId, System.currentTimeMillis());
            }
        }

    }

    @Override
    public void sendMessageAfter(SendMessageContext context) {
        String props = context.getMsgProps();
        if (props != null && props.length() > 1) {
            Map<String, String> properties = MessageDecoder.string2messageProperties(props);
            if (properties.containsKey(MessageConst.MESSAGE_CREATE_TIME)) {
                String messageTracerTimeId = properties.get(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX);
                ServerTracerTimeUtil.addMessageSaveEndTime(messageTracerTimeId, System.currentTimeMillis());
                ServerTracerTimeUtil.addBrokerSendAckTime(messageTracerTimeId, System.currentTimeMillis());
                ServerTracerTimeUtil.addBrokerSendAckTime(messageTracerTimeId, System.currentTimeMillis());
            }
        }
    }
}
