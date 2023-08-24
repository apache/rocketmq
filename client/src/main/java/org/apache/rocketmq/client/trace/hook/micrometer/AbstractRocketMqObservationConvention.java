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
package org.apache.rocketmq.client.trace.hook.micrometer;

import io.micrometer.common.KeyValues;
import io.micrometer.common.util.StringUtils;
import org.apache.rocketmq.client.trace.hook.micrometer.RocketMqObservationDocumentation.HighCardinalityTags;
import org.apache.rocketmq.client.trace.hook.micrometer.RocketMqObservationDocumentation.LowCardinalityTags;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.remoting.protocol.NamespaceUtil;

abstract class AbstractRocketMqObservationConvention {

    KeyValues getLowCardinalityKeyValues(String operation) {
        return KeyValues.of(
                LowCardinalityTags.MESSAGING_OPERATION.withValue(operation),
                LowCardinalityTags.MESSAGING_SYSTEM.withValue("rocketmq"),
                // TODO: How to discern this?
                LowCardinalityTags.NET_PROTOCOL_NAME.withValue("remoting"),
                // TODO: How do we know the version protocol?
                LowCardinalityTags.NET_PROTOCOL_VERSION.withValue("???"));
    }

    KeyValues getHighCardinalityKeyValues(Message message, String producerGroup, String brokerAddr, String namespace) {
        // TODO: Is this the same as message group?
        KeyValues keyValues = KeyValues.of(HighCardinalityTags.MESSAGING_ROCKETMQ_MESSAGE_GROUP.withValue(producerGroup != null ? producerGroup : ""), HighCardinalityTags.MESSAGING_DESTINATION_NAME.withValue(NamespaceUtil.withoutNamespace(message.getTopic())),
                HighCardinalityTags.MESSAGING_MESSAGE_PAYLOAD_SIZE_BYTES.withValue(
                        String.valueOf(message.getBody().length)),
                // TODO: Is this the correct one?
                HighCardinalityTags.NET_SOCK_PEER_ADDR.withValue(brokerAddr),
                // TODO: Should we URI.create and extract port?
                HighCardinalityTags.NET_SOCK_PEER_PORT.withValue(brokerAddr),
                HighCardinalityTags.MESSAGING_ROCKETMQ_NAMESPACE.withValue(namespace != null ? namespace : ""));
        String tags = message.getTags();
        if (StringUtils.isNotBlank(tags)) {
            keyValues = keyValues.and(HighCardinalityTags.MESSAGING_ROCKETMQ_MESSAGE_TAG.withValue(tags));
        }
        String keys = message.getKeys();
        if (StringUtils.isNotBlank(keys)) {
            keyValues = keyValues.and(keys);
        }
        String deliveryTimestamp = message.getProperties().get(MessageConst.PROPERTY_TIMER_DELIVER_MS);
        if (StringUtils.isNotBlank(deliveryTimestamp)) {
            keyValues = keyValues.and(HighCardinalityTags.MESSAGING_ROCKETMQ_MESSAGE_DELIVERY_TIMESTAMP.withValue(deliveryTimestamp));
        }
        return keyValues;
    }
}
