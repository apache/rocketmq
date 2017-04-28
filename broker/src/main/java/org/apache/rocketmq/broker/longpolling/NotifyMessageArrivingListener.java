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

import org.apache.rocketmq.store.MessageArrivingListener;

/**
 * 新消息到达监听器实现
 */
public class NotifyMessageArrivingListener implements MessageArrivingListener {

    private final PullRequestHoldService pullRequestHoldService;

    public NotifyMessageArrivingListener(final PullRequestHoldService pullRequestHoldService) {
        this.pullRequestHoldService = pullRequestHoldService;
    }

    /**
     * 新消息
     *
     * @param topic 主题
     * @param queueId 队列编号
     * @param logicOffset 队列位置
     * @param tagsCode 消息tagsCode
     */
    @Override
    public void arriving(String topic, int queueId, long logicOffset, long tagsCode) {
        this.pullRequestHoldService.notifyMessageArriving(topic, queueId, logicOffset, tagsCode);
    }

}
