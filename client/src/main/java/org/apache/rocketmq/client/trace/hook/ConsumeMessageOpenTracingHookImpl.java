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

import io.opentracing.Span;
import io.opentracing.SpanContext;
import io.opentracing.Tracer;
import io.opentracing.propagation.Format;
import io.opentracing.propagation.TextMapAdapter;
import io.opentracing.tag.Tags;
import org.apache.rocketmq.client.hook.ConsumeMessageContext;
import org.apache.rocketmq.client.hook.ConsumeMessageHook;
import org.apache.rocketmq.client.trace.TraceConstants;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.NamespaceUtil;

import java.util.ArrayList;
import java.util.List;


public class ConsumeMessageOpenTracingHookImpl implements ConsumeMessageHook {

    private Tracer tracer;

    public ConsumeMessageOpenTracingHookImpl(Tracer tracer) {
        this.tracer = tracer;
    }

    @Override
    public String hookName() {
        return "ConsumeMessageOpenTracingHook";
    }

    @Override
    public void consumeMessageBefore(ConsumeMessageContext context) {
        if (context == null || context.getMsgList() == null || context.getMsgList().isEmpty()) {
            return;
        }
        List<Span> spanList = new ArrayList<Span>();
        for (MessageExt msg : context.getMsgList()) {
            if (msg == null) {
                continue;
            }
            Tracer.SpanBuilder spanBuilder = tracer
                    .buildSpan(TraceConstants.FROM_PREFIX + msg.getTopic())
                    .withTag(Tags.SPAN_KIND, Tags.SPAN_KIND_CONSUMER);
            SpanContext spanContext = tracer.extract(Format.Builtin.TEXT_MAP, new TextMapAdapter(msg.getProperties()));
            if (spanContext != null) {
                spanBuilder.asChildOf(spanContext);
            }
            Span span = spanBuilder.start();

            span.setTag(Tags.PEER_SERVICE, TraceConstants.ROCKETMQ_SERVICE);
            span.setTag(Tags.MESSAGE_BUS_DESTINATION, NamespaceUtil.withoutNamespace(msg.getTopic()));
            span.setTag(TraceConstants.ROCKETMQ_MSG_ID, msg.getMsgId());
            span.setTag(TraceConstants.ROCKETMQ_TAGS, msg.getTags());
            span.setTag(TraceConstants.ROCKETMQ_KEYS, msg.getKeys());
            span.setTag(TraceConstants.ROCKETMQ_BODY_LENGTH, msg.getStoreSize());
            span.setTag(TraceConstants.ROCKETMQ_RETRY_TIMERS, msg.getReconsumeTimes());
            span.setTag(TraceConstants.ROCKETMQ_REGION_ID, msg.getProperty(MessageConst.PROPERTY_MSG_REGION));
            spanList.add(span);
        }
        context.setMqTraceContext(spanList);
    }

    @Override
    public void consumeMessageAfter(ConsumeMessageContext context) {
        if (context == null || context.getMsgList() == null || context.getMsgList().isEmpty()) {
            return;
        }
        List<Span> spanList = (List<Span>) context.getMqTraceContext();
        if (spanList == null) {
            return;
        }
        for (Span span : spanList) {
            span.setTag(TraceConstants.ROCKETMQ_SUCCESS, context.isSuccess());
            span.finish();
        }
    }
}
