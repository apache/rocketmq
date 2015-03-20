/**
 * Copyright (C) 2010-2013 Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.rocketmq.client.impl.producer;

import java.util.Set;

import com.alibaba.rocketmq.client.producer.TransactionCheckListener;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.protocol.header.CheckTransactionStateRequestHeader;


/**
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-7-24
 */
public interface MQProducerInner {
    Set<String> getPublishTopicList();


    boolean isPublishTopicNeedUpdate(final String topic);


    TransactionCheckListener checkListener();


    void checkTransactionState(//
                               final String addr, //
                               final MessageExt msg, //
                               final CheckTransactionStateRequestHeader checkRequestHeader);


    void updateTopicPublishInfo(final String topic, final TopicPublishInfo info);


    boolean isUnitMode();
}
