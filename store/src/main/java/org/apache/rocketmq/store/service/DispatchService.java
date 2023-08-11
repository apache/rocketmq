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
package org.apache.rocketmq.store.service;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.DispatchRequest;
import org.apache.rocketmq.store.config.BrokerRole;

public class DispatchService extends ServiceThread {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    private final List<DispatchRequest[]> dispatchRequestsList = new ArrayList<>();

    private final DefaultMessageStore messageStore;

    public DispatchService(DefaultMessageStore messageStore) {
        this.messageStore = messageStore;
    }

    // dispatchRequestsList:[
    //      {dispatchRequests:[{dispatchRequest}, {dispatchRequest}]},
    //      {dispatchRequests:[{dispatchRequest}, {dispatchRequest}]}]
    private void dispatch() {
        dispatchRequestsList.clear();
        messageStore.getDispatchRequestOrderlyQueue().get(dispatchRequestsList);
        if (dispatchRequestsList.isEmpty()) {
            return;
        }

        for (DispatchRequest[] dispatchRequests : dispatchRequestsList) {
            for (DispatchRequest dispatchRequest : dispatchRequests) {
                messageStore.doDispatch(dispatchRequest);
                activeMessageArrivingListener(dispatchRequest);
                increaseTopicCounter(dispatchRequest);
            }
        }
    }

    private void activeMessageArrivingListener(DispatchRequest dispatchRequest) {
        if (messageStore.getBrokerConfig().isLongPollingEnable()
            && messageStore.getMessageArrivingListener() != null) {
            messageStore.getMessageArrivingListener().arriving(
                dispatchRequest.getTopic(),
                dispatchRequest.getQueueId(),
                dispatchRequest.getConsumeQueueOffset() + 1,
                dispatchRequest.getTagsCode(),
                dispatchRequest.getStoreTimestamp(),
                dispatchRequest.getBitMap(),
                dispatchRequest.getPropertiesMap()
            );
            messageStore.getReputMessageService().notifyMessageArrive4MultiQueue(dispatchRequest);
        }
    }

    private void increaseTopicCounter(DispatchRequest dispatchRequest) {
        // wake up long-polling
        if (!messageStore.getMessageStoreConfig().isDuplicationEnable()
            && messageStore.getMessageStoreConfig().getBrokerRole() == BrokerRole.SLAVE) {

            messageStore.getStoreStatsService().getSinglePutMessageTopicTimesTotal(
                    dispatchRequest.getTopic())
                .add(1);
            messageStore.getStoreStatsService().getSinglePutMessageTopicSizeTotal(
                    dispatchRequest.getTopic())
                .add(dispatchRequest.getMsgSize());
        }
    }

    @Override
    public void run() {
        LOGGER.info(this.getServiceName() + " service started");

        while (!this.isStopped()) {
            try {
                TimeUnit.MILLISECONDS.sleep(1);
                dispatch();
            } catch (Exception e) {
                LOGGER.warn(this.getServiceName() + " service has exception. ", e);
            }
        }

        LOGGER.info(this.getServiceName() + " service end");
    }

    @Override
    public String getServiceName() {
        if (messageStore.getBrokerConfig().isInBrokerContainer()) {
            return messageStore.getBrokerIdentity().getIdentifier() + DispatchService.class.getSimpleName();
        }
        return DispatchService.class.getSimpleName();
    }
}

