/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.alibaba.rocketmq.client.consumer.listener;

import com.alibaba.rocketmq.common.message.MessageExt;

import java.util.List;


/**
 * A MessageListenerConcurrently object is used to receive asynchronously delivered messages orderly.one queue,one thread
 *
 * @author shijia.wxr
 */
public interface MessageListenerOrderly extends MessageListener {
    /**
     * It is not recommend to throw exception,rather than returning ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT if consumption failure
     *
     * @param msgs
     *         msgs.size() >= 1<br>
     *         DefaultMQPushConsumer.consumeMessageBatchMaxSize=1，you can modify here
     * @param context
     *
     * @return
     */
    ConsumeOrderlyStatus consumeMessage(final List<MessageExt> msgs,
                                        final ConsumeOrderlyContext context);
}
