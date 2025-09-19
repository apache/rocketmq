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
package org.apache.rocketmq.broker.sync;

import com.alibaba.fastjson2.JSON;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageClientIDSetter;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.common.sync.MetadataChangeInfo;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.PutMessageStatus;

public class SyncMessageProducer {
    protected static final Logger LOG = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    private final BrokerController brokerController;
    private final int syncQueueId;

    public SyncMessageProducer(final BrokerController brokerController) {
        this.brokerController = brokerController;
        this.syncQueueId = 0;
    }

    public void sendMetadataChange(final String targetTopic, MetadataChangeInfo changeInfo) {
        try {
            byte[] body = JSON.toJSONBytes(changeInfo);
            MessageExtBrokerInner msg = new MessageExtBrokerInner();
            msg.setBrokerName(brokerController.getBrokerConfig().getBrokerName());
            msg.setTopic(targetTopic);
            msg.setBody(body);
            msg.setKeys(changeInfo.getMetadataKey());
            msg.setTags(changeInfo.getMetadataKey());
            msg.setBornTimestamp(System.currentTimeMillis());
            msg.setStoreHost(brokerController.getStoreHost());
            msg.setBornHost(brokerController.getStoreHost());
            msg.setQueueId(this.syncQueueId);
            msg.setMsgId(MessageClientIDSetter.createUniqID());
            PutMessageResult result = this.brokerController.getMessageStore().putMessage(msg);
            if (result.getPutMessageStatus() != PutMessageStatus.PUT_OK) {
                throw new RuntimeException("send metadata change failed, topic: " + targetTopic + ", msgId: " + msg.getMsgId() + ", status: " + result.getPutMessageStatus().name());
            }
        } catch (Exception e) {
            LOG.error("send metadata change failed, topic: " + targetTopic, e);
            throw new RuntimeException(e);
        }
    }
}
