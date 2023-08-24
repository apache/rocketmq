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

package org.apache.rocketmq.client.trace;

import java.util.Map;

import io.opentracing.mock.MockSpan;
import io.opentracing.mock.MockTracer;
import io.opentracing.tag.Tags;
import org.apache.rocketmq.client.hook.SendMessageHook;
import org.apache.rocketmq.client.trace.hook.SendMessageOpenTracingHookImpl;
import org.apache.rocketmq.common.message.MessageType;

import static org.assertj.core.api.Assertions.assertThat;

public class DefaultMQProducerWithOpenTracingTest extends AbstractDefaultMQProducerTest {

    private MockTracer tracer = new MockTracer();

    @Override
    SendMessageHook hook() {
        return new SendMessageOpenTracingHookImpl(tracer);
    }

    @Override
    void assertResults() {
        assertThat(tracer.finishedSpans().size()).isEqualTo(1);
        MockSpan span = tracer.finishedSpans().get(0);
        Map<String, Object> tags = span.tags();
        assertThat(tags.get(Tags.MESSAGE_BUS_DESTINATION.getKey())).isEqualTo(topic);
        assertThat(tags.get(Tags.SPAN_KIND.getKey())).isEqualTo(Tags.SPAN_KIND_PRODUCER);
        assertThat(tags.get(TraceConstants.ROCKETMQ_MSG_ID)).isEqualTo("123");
        assertThat(tags.get(TraceConstants.ROCKETMQ_BODY_LENGTH)).isEqualTo(3);
        assertThat(tags.get(TraceConstants.ROCKETMQ_REGION_ID)).isEqualTo("HZ");
        assertThat(tags.get(TraceConstants.ROCKETMQ_MSG_TYPE)).isEqualTo(MessageType.Normal_Msg.name());
        assertThat(tags.get(TraceConstants.ROCKETMQ_SOTRE_HOST)).isEqualTo("127.0.0.1:10911");
    }

}
