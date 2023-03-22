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
import org.apache.rocketmq.client.hook.EndTransactionContext;
import org.apache.rocketmq.client.hook.EndTransactionHook;
import org.apache.rocketmq.client.trace.AsyncTraceDispatcher;
import org.apache.rocketmq.client.trace.TraceBean;
import org.apache.rocketmq.client.trace.TraceContext;
import org.apache.rocketmq.client.trace.TraceDispatcher;
import org.apache.rocketmq.client.trace.TraceType;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageType;
import org.apache.rocketmq.remoting.protocol.NamespaceUtil;

public class EndTransactionTraceHookImpl implements EndTransactionHook {

    private TraceDispatcher localDispatcher;

    public EndTransactionTraceHookImpl(TraceDispatcher localDispatcher) {
        this.localDispatcher = localDispatcher;
    }

    @Override
    public String hookName() {
        return "EndTransactionTraceHook";
    }

    @Override
    public void endTransaction(EndTransactionContext context) {
        //if it is message trace data,then it doesn't recorded
        if (context == null || context.getMessage().getTopic().startsWith(((AsyncTraceDispatcher) localDispatcher).getTraceTopicName())) {
            return;
        }
        Message msg = context.getMessage();
        //build the context content of TuxeTraceContext
        TraceContext tuxeContext = new TraceContext();
        tuxeContext.setTraceBeans(new ArrayList<>(1));
        tuxeContext.setTraceType(TraceType.EndTransaction);
        tuxeContext.setGroupName(NamespaceUtil.withoutNamespace(context.getProducerGroup()));
        //build the data bean object of message trace
        TraceBean traceBean = new TraceBean();
        traceBean.setTopic(NamespaceUtil.withoutNamespace(context.getMessage().getTopic()));
        traceBean.setTags(context.getMessage().getTags());
        traceBean.setKeys(context.getMessage().getKeys());
        traceBean.setStoreHost(context.getBrokerAddr());
        traceBean.setMsgType(MessageType.Trans_msg_Commit);
        traceBean.setClientHost(((AsyncTraceDispatcher)localDispatcher).getHostProducer().getMqClientFactory().getClientId());
        traceBean.setMsgId(context.getMsgId());
        traceBean.setTransactionState(context.getTransactionState());
        traceBean.setTransactionId(context.getTransactionId());
        traceBean.setFromTransactionCheck(context.isFromTransactionCheck());
        String regionId = msg.getProperty(MessageConst.PROPERTY_MSG_REGION);
        if (regionId == null || regionId.isEmpty()) {
            regionId = MixAll.DEFAULT_TRACE_REGION_ID;
        }
        tuxeContext.setRegionId(regionId);
        tuxeContext.getTraceBeans().add(traceBean);
        tuxeContext.setTimeStamp(System.currentTimeMillis());
        localDispatcher.append(tuxeContext);
    }

}
