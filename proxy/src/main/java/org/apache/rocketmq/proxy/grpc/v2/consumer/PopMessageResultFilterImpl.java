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
package org.apache.rocketmq.proxy.grpc.v2.consumer;

import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.common.utils.FilterUtils;
import org.apache.rocketmq.proxy.processor.PopMessageResultFilter;
import org.apache.rocketmq.remoting.protocol.heartbeat.SubscriptionData;

public class PopMessageResultFilterImpl implements PopMessageResultFilter {

    private final int maxAttempts;

    public PopMessageResultFilterImpl(int maxAttempts) {
        this.maxAttempts = maxAttempts;
    }

    @Override
    public FilterResult filterMessage(ProxyContext ctx, String consumerGroup, SubscriptionData subscriptionData,
        MessageExt messageExt) {
        if (!FilterUtils.isTagMatched(subscriptionData.getTagsSet(), messageExt.getTags())) {
            return FilterResult.NO_MATCH;
        }
        if (messageExt.getReconsumeTimes() >= maxAttempts) {
            return FilterResult.TO_DLQ;
        }
        return FilterResult.MATCH;
    }
}
