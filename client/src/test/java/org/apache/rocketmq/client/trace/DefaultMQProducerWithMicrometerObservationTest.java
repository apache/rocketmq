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

import brave.Tracing;
import brave.baggage.BaggagePropagation;
import brave.context.slf4j.MDCScopeDecorator;
import brave.handler.MutableSpan;
import brave.propagation.B3Propagation;
import brave.propagation.ThreadLocalCurrentTraceContext;
import brave.sampler.Sampler;
import brave.test.TestSpanHandler;
import io.micrometer.common.KeyValues;
import io.micrometer.core.instrument.observation.DefaultMeterObservationHandler;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.core.tck.MeterRegistryAssert;
import io.micrometer.observation.ObservationHandler.FirstMatchingCompositeObservationHandler;
import io.micrometer.observation.ObservationRegistry;
import io.micrometer.tracing.CurrentTraceContext;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.brave.bridge.BraveBaggageManager;
import io.micrometer.tracing.brave.bridge.BraveCurrentTraceContext;
import io.micrometer.tracing.brave.bridge.BravePropagator;
import io.micrometer.tracing.brave.bridge.BraveTracer;
import io.micrometer.tracing.handler.DefaultTracingObservationHandler;
import io.micrometer.tracing.handler.PropagatingReceiverTracingObservationHandler;
import io.micrometer.tracing.handler.PropagatingSenderTracingObservationHandler;
import org.apache.rocketmq.client.hook.SendMessageHook;
import org.apache.rocketmq.client.trace.hook.micrometer.SendMessageMicrometerHookImpl;
import org.junit.After;
import org.junit.Before;
import zipkin2.reporter.AsyncReporter;
import zipkin2.reporter.brave.ZipkinSpanHandler;
import zipkin2.reporter.urlconnection.URLConnectionSender;

import static org.assertj.core.api.Assertions.assertThat;

public class DefaultMQProducerWithMicrometerObservationTest extends AbstractDefaultMQProducerTest {

    ObservationRegistry observationRegistry = ObservationRegistry.create();

    SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();

    AsyncReporter reporter = AsyncReporter.create(URLConnectionSender.create("http://localhost:9411/api/v2/spans"));

    ZipkinSpanHandler spanHandler = (ZipkinSpanHandler) ZipkinSpanHandler
            .create(reporter);

    TestSpanHandler testSpanHandler = new TestSpanHandler();

    ThreadLocalCurrentTraceContext braveCurrentTraceContext = ThreadLocalCurrentTraceContext.newBuilder()
            .addScopeDecorator(MDCScopeDecorator.get()) // Example of Brave's
            // automatic MDC setup
            .build();

    CurrentTraceContext bridgeContext = new BraveCurrentTraceContext(this.braveCurrentTraceContext);

    Tracing tracing = Tracing.newBuilder()
            .currentTraceContext(this.braveCurrentTraceContext)
            .supportsJoin(false)
            .traceId128Bit(true)
            .propagationFactory(BaggagePropagation.newFactoryBuilder(B3Propagation.FACTORY)
                    .build())
            .sampler(Sampler.ALWAYS_SAMPLE)
            .addSpanHandler(this.spanHandler)
            .addSpanHandler(this.testSpanHandler)
            .localServiceName("rocketmq-client")
            .build();

    brave.Tracer braveTracer = this.tracing.tracer();

    Tracer tracer = new BraveTracer(this.braveTracer, this.bridgeContext, new BraveBaggageManager());

    BravePropagator propagator = new BravePropagator(this.tracing);

    @Before
    public void setupObservationRegistry() {
        // metrics
        this.observationRegistry.observationConfig().observationHandler(new DefaultMeterObservationHandler(this.meterRegistry));
        // tracing
        this.observationRegistry.observationConfig().observationHandler(new FirstMatchingCompositeObservationHandler(new PropagatingSenderTracingObservationHandler<>(this.tracer, this.propagator), new PropagatingReceiverTracingObservationHandler<>(this.tracer, this.propagator), new DefaultTracingObservationHandler(this.tracer)));
    }

    @Override
    SendMessageHook hook() {
        return new SendMessageMicrometerHookImpl(this.observationRegistry, null);
    }

    @Override
    void assertResults() {
        assertTracing();
        assertMetrics();
    }

    private void assertTracing() {
        List<MutableSpan> spans = testSpanHandler.spans();
        assertThat(spans).hasSize(1);
        MutableSpan span = spans.get(0);
        Map<String, String> tags = span.tags();
        assertThat(tags).isNotEmpty();
    }

    private void assertMetrics() {
        MeterRegistryAssert.assertThat(meterRegistry)
                .hasTimerWithNameAndTags("rocketmq.publish",
                        KeyValues.of("error", "none", "messaging.operation", "publish", "messaging.rocketmq.message.type", "Normal", "messaging.system", "rocketmq", "net.protocol.name", "remoting", "net.protocol.version", "???")
                )
                .hasMeterWithName("rocketmq.publish.active");
    }

    @After
    public void clear() throws InterruptedException {
        reporter.flush();
        reporter.close();
        spanHandler.close();
        tracing.close();
    }
}
