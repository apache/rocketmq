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

import org.apache.rocketmq.client.trace.TraceBean;
import org.apache.rocketmq.client.trace.TraceContext;
import org.apache.rocketmq.client.trace.TraceDispatcher;
import org.apache.rocketmq.client.trace.TraceType;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.producer.RecallMessageHandle;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.protocol.NamespaceUtil;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.header.RecallMessageRequestHeader;

import java.util.ArrayList;

public class DefaultRecallMessageTraceHook implements RPCHook {

    private static final String RECALL_TRACE_ENABLE_KEY = "com.rocketmq.recall.default.trace.enable";
    private boolean enableDefaultTrace = Boolean.parseBoolean(System.getProperty(RECALL_TRACE_ENABLE_KEY, "false"));
    private TraceDispatcher traceDispatcher;

    public DefaultRecallMessageTraceHook(TraceDispatcher traceDispatcher) {
        this.traceDispatcher = traceDispatcher;
    }

    @Override
    public void doBeforeRequest(String remoteAddr, RemotingCommand request) {
    }

    @Override
    public void doAfterResponse(String remoteAddr, RemotingCommand request, RemotingCommand response) {
        if (request.getCode() != RequestCode.RECALL_MESSAGE
            || !enableDefaultTrace
            || null == response.getExtFields()
            || null == response.getExtFields().get(MessageConst.PROPERTY_MSG_REGION)
            || null == traceDispatcher) {
            return;
        }

        try {
            String regionId = response.getExtFields().get(MessageConst.PROPERTY_MSG_REGION);
            RecallMessageRequestHeader requestHeader =
                request.decodeCommandCustomHeader(RecallMessageRequestHeader.class);
            String topic = NamespaceUtil.withoutNamespace(requestHeader.getTopic());
            String group = NamespaceUtil.withoutNamespace(requestHeader.getProducerGroup());
            String recallHandle = requestHeader.getRecallHandle();
            RecallMessageHandle.HandleV1 handleV1 =
                (RecallMessageHandle.HandleV1) RecallMessageHandle.decodeHandle(recallHandle);

            TraceBean traceBean = new TraceBean();
            traceBean.setTopic(topic);
            traceBean.setMsgId(handleV1.getMessageId());

            TraceContext traceContext = new TraceContext();
            traceContext.setRegionId(regionId);
            traceContext.setTraceBeans(new ArrayList<>(1));
            traceContext.setTraceType(TraceType.Recall);
            traceContext.setGroupName(group);
            traceContext.getTraceBeans().add(traceBean);
            traceContext.setSuccess(ResponseCode.SUCCESS == response.getCode());

            traceDispatcher.append(traceContext);
        } catch (Exception e) {
        }
    }
}
