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
 * 消息过滤实现
 */
public class DefaultMessageFilter implements MessageFilter {

    @Override
    public boolean isMessageMatched(SubscriptionData subscriptionData, Long tagsCode) {
        // 消息tagsCode 空
        if (tagsCode == null) {
            return true;
        }
        // 订阅数据 空
        if (null == subscriptionData) {
            return true;
        }
        // classFilter
        if (subscriptionData.isClassFilterMode())
            return true;
        // 订阅表达式 全匹配
        if (subscriptionData.getSubString().equals(SubscriptionData.SUB_ALL)) {
            return true;
        }
        // 订阅数据code数组 是否包含 消息tagsCode
        return subscriptionData.getCodeSet().contains(tagsCode.intValue());
    }

}
