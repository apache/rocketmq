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
package io.openmessaging.rocketmq;

import io.openmessaging.BytesMessage;
import io.openmessaging.KeyValue;
import io.openmessaging.MessageHeader;
import io.openmessaging.OMS;
import io.openmessaging.SendResult;
import io.openmessaging.rocketmq.domain.SendResultImpl;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.message.MessageAccessor;

public class OMSUtil {

    /**
     * Builds a OMS client instance name.
     *
     * @return a unique instance name
     */
    public static String buildInstanceName() {
        return Integer.toString(UtilAll.getPid()) + "%OpenMessaging" + "%" + System.nanoTime();
    }

    public static org.apache.rocketmq.common.message.Message msgConvert(BytesMessage omsMessage) {
        org.apache.rocketmq.common.message.Message rmqMessage = new org.apache.rocketmq.common.message.Message();
        rmqMessage.setBody(omsMessage.getBody());

        KeyValue headers = omsMessage.headers();
        KeyValue properties = omsMessage.properties();

        //All destinations in RocketMQ use Topic
        rmqMessage.setTopic(headers.containsKey(MessageHeader.TOPIC)
            ? headers.getString(MessageHeader.TOPIC) : headers.getString(MessageHeader.QUEUE));

        for (String key : properties.keySet()) {
            MessageAccessor.putProperty(rmqMessage, key, properties.getString(key));
        }

        //Headers has a high priority
        for (String key : headers.keySet()) {
            MessageAccessor.putProperty(rmqMessage, key, headers.getString(key));
        }

        return rmqMessage;
    }

    /**
     * Convert a RocketMQ SEND_OK SendResult instance to a OMS SendResult.
     */
    public static SendResult sendResultConvert(org.apache.rocketmq.client.producer.SendResult rmqResult) {
        assert rmqResult.getSendStatus().equals(SendStatus.SEND_OK);
        return new SendResultImpl(rmqResult.getMsgId(), OMS.newKeyValue());
    }

    public static KeyValue buildKeyValue(KeyValue ... keyValues) {
        KeyValue keyValue = OMS.newKeyValue();
        for (KeyValue properties : keyValues) {
            for (String key : properties.keySet()) {
                keyValue.put(key, properties.getString(key));
            }
        }
        return keyValue;
    }
}
