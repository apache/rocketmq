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
package org.apache.rocketmq.client.opentracing.hook;

import com.google.common.collect.ImmutableMap;
import io.opentracing.Span;
import io.opentracing.Tracer;
import org.apache.rocketmq.client.hook.SendMessageContext;
import org.apache.rocketmq.client.hook.SendMessageHook;

public class SendMessageOpenTracingHookImpl   implements SendMessageHook {
    private Span rootSpan;
    private Span span;
    private Tracer tracer;

    public SendMessageOpenTracingHookImpl(Span rootSpan,Tracer tracer) {
        this.rootSpan = rootSpan;
        this.tracer = tracer;

    }

    @Override
    public String hookName() {
        return "SendMessageOpenTracingHook";
    }

    @Override
    public void sendMessageBefore(SendMessageContext context) {
        span = tracer.buildSpan("sendMessage").asChildOf(rootSpan).start();

    }

    @Override
    public void sendMessageAfter(SendMessageContext context) {
        span.log(ImmutableMap.of("topic",context.getSendResult().getMessageQueue().getTopic(),"msgId",context.getSendResult().getMsgId(),
            "offsetMsgId",context.getSendResult().getOffsetMsgId(),"tag",context.getMessage().getTags(),"key",context.getMessage().getKeys()));
        span.log(ImmutableMap.of("msgType",context.getMsgType(),"bodyLength",context.getMessage().getBody().length));
        span.finish();
    }
}
