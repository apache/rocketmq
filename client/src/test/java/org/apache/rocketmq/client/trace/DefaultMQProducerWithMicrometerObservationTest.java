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

import java.util.List;
import java.util.Map;

import brave.handler.MutableSpan;
import io.micrometer.common.KeyValues;
import io.micrometer.core.tck.MeterRegistryAssert;
import org.apache.rocketmq.client.hook.SendMessageHook;
import org.apache.rocketmq.client.trace.hook.micrometer.SendMessageMicrometerHookImpl;
import org.junit.After;

import static org.assertj.core.api.Assertions.assertThat;

public class DefaultMQProducerWithMicrometerObservationTest extends AbstractDefaultMQProducerTest implements MicrometerObservationAware {

    ObservationSetup observationSetup = new ObservationSetup();

    @Override
    SendMessageHook hook() {
        return new SendMessageMicrometerHookImpl(getObservationRegistry(observationSetup), null);
    }

    @Override
    void assertResults() {
        assertTracing();
        assertMetrics();
    }

    private void assertTracing() {
        List<MutableSpan> spans = getFinishedSpans(observationSetup);
        assertThat(spans).hasSize(1);
        MutableSpan span = spans.get(0);
        Map<String, String> tags = span.tags();
        assertThat(tags).isNotEmpty();
    }

    private void assertMetrics() {
        MeterRegistryAssert.assertThat(getMeterRegistry(observationSetup))
                .hasTimerWithNameAndTags("rocketmq.publish",
                        KeyValues.of("error", "none", "messaging.operation", "publish", "messaging.rocketmq.message.type", "Normal", "messaging.system", "rocketmq", "net.protocol.name", "remoting", "net.protocol.version", "???")
                )
                .hasMeterWithName("rocketmq.publish.active");
    }

    @After
    public void clean() {
        clearResources(observationSetup);
    }
}
