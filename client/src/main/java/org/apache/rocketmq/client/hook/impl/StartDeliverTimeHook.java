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
package org.apache.rocketmq.client.hook.impl;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.consumer.MQConsumer;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.hook.CheckForbiddenContext;
import org.apache.rocketmq.client.hook.CheckForbiddenHook;
import org.apache.rocketmq.client.hook.FilterMessageContext;
import org.apache.rocketmq.client.hook.FilterMessageHook;
import org.apache.rocketmq.client.utils.MessageUtil;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageBatch;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.common.RemotingHelper;

public class StartDeliverTimeHook implements CheckForbiddenHook, FilterMessageHook {
    
    private MQConsumer consumer;
    
    public MQConsumer getConsumer() {
        return consumer;
    }

    public void setConsumer(MQConsumer consumer) {
        this.consumer = consumer;
    }

    @Override
    public String hookName() {
        return StartDeliverTimeHook.class.getName();
    }

    @Override
    public void checkForbidden(CheckForbiddenContext context) throws MQClientException {
        Message msg = context.getMessage();
        boolean msgBatch = msg instanceof MessageBatch ;
        if (!msgBatch) {
            String startDeliverTimeMillis = MessageAccessor.getConsumeStartTimeStamp(msg);
            if (StringUtils.isNoneBlank(startDeliverTimeMillis)) {
                long delayTimeMillis = Long.parseLong(startDeliverTimeMillis) - System.currentTimeMillis();
                int level = MessageUtil.calcDelayTimeLevel(delayTimeMillis, context.getBrokerAddr(), context.getProducer().getmQClientFactory().getMQClientAPIImpl());
                if (level > 0) {
                    msg.setDelayTimeLevel(level);
                } else {
                    MessageAccessor.clearProperty(msg, MessageConst.PROPERTY_CONSUME_START_TIMESTAMP);
                }
            }
        }
        
    }

    @Override
    public void filterMessage(FilterMessageContext context) throws MQClientException {
        List<MessageExt> msgList = context.getMsgList();
        for (Iterator<MessageExt> iter = msgList.iterator(); iter.hasNext();) {
            MessageExt msg = iter.next();
            long startDeliverTime = msg.getStartDeliverTime();
            long delayTimeMillis = startDeliverTime - System.currentTimeMillis();
            if (delayTimeMillis > 0L) {
                String brokerAddr = RemotingHelper.parseSocketAddressAddr(msg.getStoreHost());
                int level = 0;
                try {
                    level = MessageUtil.calcDelayTimeLevel(delayTimeMillis, brokerAddr, context.getMQClientFactory().getMQClientAPIImpl());
                } catch (Throwable e) {
                    throw new MQClientException("calcDelayTimeLevel fail", e);
                }
                if (level > 0) {
                    try {
                        consumer.sendMessageBack(msg, level, null);
                        iter.remove();
                    } catch (Throwable e) {
                        throw new MQClientException("filterMessage sendMessageBack fail", e); 
                    }
                } else {
                    try {
                        TimeUnit.MILLISECONDS.sleep(delayTimeMillis);
                    } catch (Throwable e) {
                        throw new MQClientException("filterMessage sleep fail", e);
                    }
                }
            }
        }
    }

}
