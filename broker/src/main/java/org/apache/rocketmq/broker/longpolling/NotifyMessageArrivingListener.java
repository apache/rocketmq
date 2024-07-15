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

package org.apache.rocketmq.broker.longpolling;

import java.util.Map;
import org.apache.rocketmq.broker.processor.NotificationProcessor;
import org.apache.rocketmq.broker.processor.PopMessageProcessor;
import org.apache.rocketmq.store.MessageArrivingListener;

public class NotifyMessageArrivingListener implements MessageArrivingListener {
    private final PullRequestHoldService pullRequestHoldService;
    private final PopMessageProcessor popMessageProcessor;
    private final NotificationProcessor notificationProcessor;

    public NotifyMessageArrivingListener(final PullRequestHoldService pullRequestHoldService, final PopMessageProcessor popMessageProcessor, final NotificationProcessor notificationProcessor) {
        this.pullRequestHoldService = pullRequestHoldService;
        this.popMessageProcessor = popMessageProcessor;
        this.notificationProcessor = notificationProcessor;
    }

    @Override
    public void arriving(String topic, int queueId, long logicOffset, long tagsCode,
                         long msgStoreTime, byte[] filterBitMap, Map<String, String> properties) {

        this.pullRequestHoldService.notifyMessageArriving(
            topic, queueId, logicOffset, tagsCode, msgStoreTime, filterBitMap, properties);
        this.popMessageProcessor.notifyMessageArriving(
            topic, queueId, tagsCode, msgStoreTime, filterBitMap, properties);
        this.notificationProcessor.notifyMessageArriving(
            topic, queueId, tagsCode, msgStoreTime, filterBitMap, properties);
    }
}
