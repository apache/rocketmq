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
package org.apache.rocketmq.client.trace.core.hook;

import org.apache.rocketmq.client.hook.SendMessageContext;
import org.apache.rocketmq.client.hook.SendMessageHook;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.client.trace.core.common.TrackTraceBean;
import org.apache.rocketmq.client.trace.core.common.TrackTraceConstants;
import org.apache.rocketmq.client.trace.core.common.TrackTraceContext;
import org.apache.rocketmq.client.trace.core.common.TrackTraceType;
import org.apache.rocketmq.client.trace.core.dispatch.AsyncDispatcher;
import org.apache.rocketmq.common.MixAll;
import java.util.ArrayList;

public class SendMessageTrackHookImpl implements SendMessageHook {

    private AsyncDispatcher localDispatcher;

    public SendMessageTrackHookImpl(AsyncDispatcher localDispatcher) {
        this.localDispatcher = localDispatcher;
    }

    @Override
    public String hookName() {
        return "SendMessageTrackHook";
    }

    @Override
    public void sendMessageBefore(SendMessageContext context) {
        //if it is message track trace data,then it doesn't recorded
        if (context == null || context.getMessage().getTopic().startsWith(MixAll.SYSTEM_TOPIC_PREFIX)) {
            return;
        }
        //build the context content of TuxeTraceContext
        TrackTraceContext tuxeContext = new TrackTraceContext();
        tuxeContext.setTraceBeans(new ArrayList<TrackTraceBean>(1));
        context.setMqTraceContext(tuxeContext);
        tuxeContext.setTraceType(TrackTraceType.Pub);
        tuxeContext.setGroupName(context.getProducerGroup());

        //build the data bean object of message track trace
        TrackTraceBean traceBean = new TrackTraceBean();
        traceBean.setTopic(context.getMessage().getTopic());
        traceBean.setTags(context.getMessage().getTags());
        traceBean.setKeys(context.getMessage().getKeys());
        traceBean.setStoreHost(context.getBrokerAddr());
        traceBean.setBodyLength(context.getMessage().getBody().length);
        traceBean.setMsgType(context.getMsgType());
        tuxeContext.getTraceBeans().add(traceBean);
    }

    @Override
    public void sendMessageAfter(SendMessageContext context) {
        //if it is message track trace data,then it doesn't recorded
        if (context == null || context.getMessage().getTopic().startsWith(TrackTraceConstants.TRACE_TOPIC)
            || context.getMqTraceContext() == null) {
            return;
        }
        if (context.getSendResult() == null) {
            return;
        }

        if (context.getSendResult().getRegionId() == null
            || !context.getSendResult().isTraceOn()) {
            // if switch is false,skip it
            return;
        }

        TrackTraceContext tuxeContext = (TrackTraceContext) context.getMqTraceContext();
        TrackTraceBean traceBean = tuxeContext.getTraceBeans().get(0);
        int costTime = (int) ((System.currentTimeMillis() - tuxeContext.getTimeStamp()) / tuxeContext.getTraceBeans().size());
        tuxeContext.setCostTime(costTime);
        if (context.getSendResult().getSendStatus().equals(SendStatus.SEND_OK)) {
            tuxeContext.setSuccess(true);
        } else {
            tuxeContext.setSuccess(false);
        }
        tuxeContext.setRegionId(context.getSendResult().getRegionId());
        traceBean.setMsgId(context.getSendResult().getMsgId());
        traceBean.setOffsetMsgId(context.getSendResult().getOffsetMsgId());
        traceBean.setStoreTime(tuxeContext.getTimeStamp() + costTime / 2);
        localDispatcher.append(tuxeContext);
    }
}
