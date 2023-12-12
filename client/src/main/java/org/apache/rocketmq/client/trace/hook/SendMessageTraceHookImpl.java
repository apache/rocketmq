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
package org.apache.rocketmq.client.trace.hook;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.hook.SendMessageContext;
import org.apache.rocketmq.client.hook.SendMessageHook;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.client.trace.AsyncTraceDispatcher;
import org.apache.rocketmq.client.trace.TraceBean;
import org.apache.rocketmq.client.trace.TraceContext;
import org.apache.rocketmq.client.trace.TraceDispatcher;
import org.apache.rocketmq.client.trace.TraceType;
import org.apache.rocketmq.remoting.protocol.NamespaceUtil;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageBatch;
import org.apache.rocketmq.common.message.MessageType;

public class SendMessageTraceHookImpl implements SendMessageHook {

    private TraceDispatcher localDispatcher;

    public SendMessageTraceHookImpl(TraceDispatcher localDispatcher) {
        this.localDispatcher = localDispatcher;
    }

    @Override
    public String hookName() {
        return "SendMessageTraceHook";
    }

    @Override
    public void sendMessageBefore(SendMessageContext context) {
        //if it is message trace data,then it doesn't recorded
        if (context == null || context.getMessage().getTopic().startsWith(((AsyncTraceDispatcher) localDispatcher).getTraceTopicName())) {
            return;
        }
        //build the context content of TraceContext
        TraceContext traceContext = new TraceContext();
        Message message = context.getMessage();
        List<TraceBean> traceBeans;
        if (message instanceof MessageBatch) {
            MessageBatch messageBatch = (MessageBatch) message;
            traceBeans = new ArrayList<>(messageBatch.getBatchCount());
            for (Message batch : messageBatch) {
                traceBeans.add(buildBeforeTraceBean(batch, context.getMsgType(), context.getBrokerAddr()));
            }
        } else {
            traceBeans = new ArrayList<>(1);
            traceBeans.add(buildBeforeTraceBean(message, context.getMsgType(), context.getBrokerAddr()));
        }

        traceContext.setTraceBeans(traceBeans);
        context.setMqTraceContext(traceContext);
        traceContext.setTraceType(TraceType.Pub);
        traceContext.setGroupName(NamespaceUtil.withoutNamespace(context.getProducerGroup()));
    }

    public TraceBean buildBeforeTraceBean(Message message, MessageType msgType, String brokerAddr) {
        //build the data bean object of message trace
        TraceBean traceBean = new TraceBean();
        traceBean.setTopic(NamespaceUtil.withoutNamespace(message.getTopic()));
        traceBean.setTags(message.getTags());
        traceBean.setKeys(message.getKeys());
        traceBean.setStoreHost(brokerAddr);
        traceBean.setBodyLength(message.getBody().length);
        traceBean.setMsgType(msgType);
        return traceBean;
    }

    @Override
    public void sendMessageAfter(SendMessageContext context) {
        //if it is message trace data,then it doesn't recorded
        if (context == null || context.getMessage().getTopic().startsWith(((AsyncTraceDispatcher) localDispatcher).getTraceTopicName())
            || context.getMqTraceContext() == null) {
            return;
        }
        if (context.getSendResult() == null) {
            return;
        }

        if (context.getSendResult().getRegionId() == null
            || !context.getSendResult().isTraceOn()
            || StringUtils.isEmpty(context.getSendResult().getMsgId())
            || StringUtils.isEmpty(context.getSendResult().getOffsetMsgId())) {
            // if switch is false,skip it
            return;
        }

        TraceContext traceContext = (TraceContext) context.getMqTraceContext();
        String[] uniqMsgIds = context.getSendResult().getMsgId().split(",");
        String[] offsetMsgIds = context.getSendResult().getOffsetMsgId().split(",");
        if (uniqMsgIds.length != traceContext.getTraceBeans().size() || offsetMsgIds.length != traceContext.getTraceBeans().size()) {
            return;
        }
        int costTime = (int) (System.currentTimeMillis() - traceContext.getTimeStamp());
        for (int i = 0; i < traceContext.getTraceBeans().size(); i++) {
            // build traceBean
            TraceBean traceBean = traceContext.getTraceBeans().get(i);
            traceBean.setMsgId(uniqMsgIds[i]);
            traceBean.setOffsetMsgId(offsetMsgIds[i]);
            traceBean.setStoreTime(traceContext.getTimeStamp() + costTime / 2);

            // build traceContext
            TraceContext tmpContext = new TraceContext();
            tmpContext.setTraceType(traceContext.getTraceType());
            tmpContext.setRegionId(context.getSendResult().getRegionId());
            tmpContext.setGroupName(traceContext.getGroupName());
            tmpContext.setCostTime(costTime);
            tmpContext.setSuccess(context.getSendResult().getSendStatus().equals(SendStatus.SEND_OK));
            tmpContext.setContextCode(traceContext.getContextCode());
            tmpContext.setTraceBeans(new ArrayList<>(1));
            tmpContext.getTraceBeans().add(traceBean);
            localDispatcher.append(tmpContext);
        }
    }
}
