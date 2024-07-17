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
import org.apache.rocketmq.client.hook.SendMessageContext;
import org.apache.rocketmq.client.hook.SendMessageHook;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.client.trace.TraceConstants;
import org.apache.rocketmq.common.message.Message;

public class SendMessageOpenTracingHookImpl implements SendMessageHook {

    private Tracer tracer;

    public SendMessageOpenTracingHookImpl(Tracer tracer) {
        this.tracer = tracer;
    }

    @Override
    public String hookName() {
        return "SendMessageOpenTracingHook";
    }

    @Override
    public void sendMessageBefore(SendMessageContext context) {
        if (context == null) {
            return;
        }
        Message msg = context.getMessage();
        Tracer.SpanBuilder spanBuilder = tracer
                .buildSpan(TraceConstants.TO_PREFIX + msg.getTopic())
                .withTag(Tags.SPAN_KIND, Tags.SPAN_KIND_PRODUCER);
        SpanContext spanContext = tracer.extract(Format.Builtin.TEXT_MAP, new TextMapAdapter(msg.getProperties()));
        if (spanContext != null) {
            spanBuilder.asChildOf(spanContext);
        }
        Span span = spanBuilder.start();
        tracer.inject(span.context(), Format.Builtin.TEXT_MAP, new TextMapAdapter(msg.getProperties()));
        span.setTag(Tags.PEER_SERVICE, TraceConstants.ROCKETMQ_SERVICE);
        span.setTag(Tags.MESSAGE_BUS_DESTINATION, msg.getTopic());
        span.setTag(TraceConstants.ROCKETMQ_TAGS, msg.getTags());
        span.setTag(TraceConstants.ROCKETMQ_KEYS, msg.getKeys());
        span.setTag(TraceConstants.ROCKETMQ_STORE_HOST, context.getBrokerAddr());
        span.setTag(TraceConstants.ROCKETMQ_MSG_TYPE, context.getMsgType().name());
        span.setTag(TraceConstants.ROCKETMQ_BODY_LENGTH, msg.getBody().length);
        context.setMqTraceContext(span);
    }

    @Override
    public void sendMessageAfter(SendMessageContext context) {
        if (context == null || context.getMqTraceContext() == null) {
            return;
        }
        if (context.getSendResult() == null) {
            return;
        }

        if (context.getSendResult().getRegionId() == null) {
            return;
        }

        Span span = (Span) context.getMqTraceContext();
        span.setTag(TraceConstants.ROCKETMQ_SUCCESS, context.getSendResult().getSendStatus().equals(SendStatus.SEND_OK));
        span.setTag(TraceConstants.ROCKETMQ_MSG_ID, context.getSendResult().getMsgId());
        span.setTag(TraceConstants.ROCKETMQ_REGION_ID, context.getSendResult().getRegionId());
        span.finish();
    }
}
