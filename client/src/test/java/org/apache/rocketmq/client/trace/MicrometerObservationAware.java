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

import brave.Tracing;
import brave.baggage.BaggagePropagation;
import brave.context.slf4j.MDCScopeDecorator;
import brave.handler.MutableSpan;
import brave.propagation.B3Propagation;
import brave.propagation.ThreadLocalCurrentTraceContext;
import brave.sampler.Sampler;
import brave.test.TestSpanHandler;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.observation.DefaultMeterObservationHandler;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
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
import org.junit.After;
import zipkin2.reporter.AsyncReporter;
import zipkin2.reporter.brave.ZipkinSpanHandler;
import zipkin2.reporter.urlconnection.URLConnectionSender;

/**
 * Abstrats setting up of {@link ObservationRegistry} with Brave for tracing and Micrometer for metrics.
 */
interface MicrometerObservationAware {

    /**
     * {@link ObservationRegistry} setup.
     */
    class ObservationSetup {
        boolean alreadySetup;

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
    }

    /**
     * Sets up the {@link ObservationRegistry}.
     * @param observationSetup observation setup
     */
    default void setupObservationRegistry(ObservationSetup observationSetup) {
        if (!observationSetup.alreadySetup) {
            // metrics
            observationSetup.observationRegistry.observationConfig().observationHandler(new DefaultMeterObservationHandler(observationSetup.meterRegistry));
            // tracing
            observationSetup.observationRegistry.observationConfig().observationHandler(new FirstMatchingCompositeObservationHandler(new PropagatingSenderTracingObservationHandler<>(observationSetup.tracer, observationSetup.propagator), new PropagatingReceiverTracingObservationHandler<>(observationSetup.tracer, observationSetup.propagator), new DefaultTracingObservationHandler(observationSetup.tracer)));
            observationSetup.alreadySetup = true;
        }
    }

    /**
     * Returns finished spans.
     * @param observationSetup observation setup
     * @return finished spans
     */
    default List<MutableSpan> getFinishedSpans(ObservationSetup observationSetup) {
        return observationSetup.testSpanHandler.spans();
    }

    /**
     * Returns the pre-configured {@link ObservationRegistry}.
     * @param observationSetup observation setup
     * @return {@link ObservationRegistry}
     */
    default ObservationRegistry getObservationRegistry(ObservationSetup observationSetup) {
        setupObservationRegistry(observationSetup);
        return observationSetup.observationRegistry;
    }

    /**
     * Returns {@link MeterRegistry}.
     * @param observationSetup observation setup
     * @return {@link MeterRegistry}
     */
    default MeterRegistry getMeterRegistry(ObservationSetup observationSetup) {
        return observationSetup.meterRegistry;
    }

    /**
     * Cleans up all resources. Must be called in {@link After} annotated method.
     * @param observationSetup observation setup
     */
    default void clearResources(ObservationSetup observationSetup) {
        observationSetup.reporter.flush();
        observationSetup.reporter.close();
        observationSetup.spanHandler.close();
        observationSetup.tracing.close();
    }
}
