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
package org.apache.rocketmq.store;

import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;

/**
 * 消息过滤接口
 */
public interface MessageFilter {

    /**
     * 消息是否匹配
     *
     * @param subscriptionData 订阅数据
     * @param tagsCode 消息tagsCode
     * @return 是否匹配
     */
    boolean isMessageMatched(final SubscriptionData subscriptionData, final Long tagsCode);
}
