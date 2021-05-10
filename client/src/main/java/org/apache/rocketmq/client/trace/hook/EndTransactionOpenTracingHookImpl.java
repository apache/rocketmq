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
import org.apache.rocketmq.client.hook.EndTransactionContext;
import org.apache.rocketmq.client.hook.EndTransactionHook;
import org.apache.rocketmq.client.trace.TraceConstants;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageType;

public class EndTransactionOpenTracingHookImpl implements EndTransactionHook {

    private Tracer tracer;

    public EndTransactionOpenTracingHookImpl(Tracer tracer) {
        this.tracer = tracer;
    }

    @Override
    public String hookName() {
        return "EndTransactionOpenTracingHook";
    }

    @Override
    public void endTransaction(EndTransactionContext context) {
        if (context == null) {
            return;
        }
        Message msg = context.getMessage();
        Tracer.SpanBuilder spanBuilder = tracer
                .buildSpan(TraceConstants.END_TRANSACTION)
                .withTag(Tags.SPAN_KIND, Tags.SPAN_KIND_PRODUCER);
        SpanContext spanContext = tracer.extract(Format.Builtin.TEXT_MAP, new TextMapAdapter(msg.getProperties()));
        if (spanContext != null) {
            spanBuilder.asChildOf(spanContext);
        }

        Span span = spanBuilder.start();
        span.setTag(Tags.PEER_SERVICE, TraceConstants.ROCKETMQ_SERVICE);
        span.setTag(Tags.MESSAGE_BUS_DESTINATION, msg.getTopic());
        span.setTag(TraceConstants.ROCKETMQ_TAGS, msg.getTags());
        span.setTag(TraceConstants.ROCKETMQ_KEYS, msg.getKeys());
        span.setTag(TraceConstants.ROCKETMQ_SOTRE_HOST, context.getBrokerAddr());
        span.setTag(TraceConstants.ROCKETMQ_MSG_ID, context.getMsgId());
        span.setTag(TraceConstants.ROCKETMQ_MSG_TYPE, MessageType.Trans_msg_Commit.name());
        span.setTag(TraceConstants.ROCKETMQ_TRANSACTION_ID, context.getTransactionId());
        span.setTag(TraceConstants.ROCKETMQ_TRANSACTION_STATE, context.getTransactionState().name());
        span.setTag(TraceConstants.ROCKETMQ_IS_FROM_TRANSACTION_CHECK, context.isFromTransactionCheck());
        span.finish();
    }

}
