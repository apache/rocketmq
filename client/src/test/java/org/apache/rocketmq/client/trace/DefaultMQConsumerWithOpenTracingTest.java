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

import io.opentracing.mock.MockSpan;
import io.opentracing.mock.MockTracer;
import io.opentracing.tag.Tags;
import org.apache.rocketmq.client.hook.ConsumeMessageHook;
import org.apache.rocketmq.client.trace.hook.ConsumeMessageOpenTracingHookImpl;

import static org.assertj.core.api.Assertions.assertThat;

public class DefaultMQConsumerWithOpenTracingTest extends AbstractDefaultMQConsumerTest {

    private final MockTracer tracer = new MockTracer();

    @Override
    ConsumeMessageHook consumeMessageHook() {
        return new ConsumeMessageOpenTracingHookImpl(tracer);
    }

    @Override
    boolean waitCondition() {
        return tracer.finishedSpans().size() == 1;
    }

    @Override
    void assertResults() {
        MockSpan span = tracer.finishedSpans().get(0);
        assertThat(span.tags().get(Tags.MESSAGE_BUS_DESTINATION.getKey())).isEqualTo(topic);
        assertThat(span.tags().get(Tags.SPAN_KIND.getKey())).isEqualTo(Tags.SPAN_KIND_CONSUMER);
        assertThat(span.tags().get(TraceConstants.ROCKETMQ_SUCCESS)).isEqualTo(true);
    }
}
