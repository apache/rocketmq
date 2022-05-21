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

package org.apache.rocketmq.thinclient.metrics;

import io.github.aliyunmq.shaded.org.slf4j.Logger;
import io.github.aliyunmq.shaded.org.slf4j.LoggerFactory;
import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContext;
import io.grpc.netty.shaded.io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.exporter.otlp.metrics.OtlpGrpcMetricExporter;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.Aggregation;
import io.opentelemetry.sdk.metrics.InstrumentSelector;
import io.opentelemetry.sdk.metrics.InstrumentType;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.View;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import org.apache.rocketmq.apis.consumer.PushConsumer;
import org.apache.rocketmq.apis.consumer.SimpleConsumer;
import org.apache.rocketmq.thinclient.impl.ClientImpl;
import org.apache.rocketmq.thinclient.route.Endpoints;
import org.apache.rocketmq.thinclient.rpc.AuthInterceptor;
import org.apache.rocketmq.thinclient.rpc.IpNameResolverFactory;

public class MessageMeter {
    private static final Logger LOGGER = LoggerFactory.getLogger(MessageMeter.class);

    private static final Duration METRIC_EXPORTER_RPC_TIMEOUT = Duration.ofSeconds(3);

    private static final String METRIC_INSTRUMENTATION_NAME = "org.apache.rocketmq.message";

    private final ClientImpl client;

    private volatile Meter meter;
    private volatile Endpoints metricEndpoints;
    private volatile SdkMeterProvider provider;

    private volatile MessageCacheObserver messageCacheObserver;

    private final ConcurrentMap<MetricName, LongCounter> counterMap;
    private final ConcurrentMap<MetricName, DoubleHistogram> histogramMap;

    public MessageMeter(ClientImpl client) {
        this.client = client;
        this.counterMap = new ConcurrentHashMap<>();
        this.histogramMap = new ConcurrentHashMap<>();
        this.client.registerMessageInterceptor(new MetricMessageInterceptor(this));
        this.messageCacheObserver = null;
    }

    public void setMessageCacheObserver(MessageCacheObserver messageCacheObserver) {
        this.messageCacheObserver = messageCacheObserver;
    }

    LongCounter getSendSuccessTotalCounter() {
        return counterMap.computeIfAbsent(MetricName.SEND_SUCCESS_TOTAL, name -> meter.counterBuilder(name.getName()).build());
    }

    LongCounter getSendFailureTotalCounter() {
        return counterMap.computeIfAbsent(MetricName.SEND_FAILURE_TOTAL, name -> meter.counterBuilder(name.getName()).build());
    }

    DoubleHistogram getSendSuccessCostTimeHistogram() {
        return histogramMap.computeIfAbsent(MetricName.SEND_SUCCESS_COST_TIME, name -> meter.histogramBuilder(name.getName()).build());
    }

    LongCounter getProcessSuccessTotalCounter() {
        return counterMap.computeIfAbsent(MetricName.PROCESS_SUCCESS_TOTAL, name -> meter.counterBuilder(name.getName()).build());
    }

    LongCounter getProcessFailureTotalCounter() {
        return counterMap.computeIfAbsent(MetricName.PROCESS_FAILURE_TOTAL, name -> meter.counterBuilder(name.getName()).build());
    }

    DoubleHistogram getProcessCostTimeHistogram() {
        return histogramMap.computeIfAbsent(MetricName.PROCESS_TIME, name -> meter.histogramBuilder(name.getName()).build());
    }

    LongCounter getAckSuccessTotalCounter() {
        return counterMap.computeIfAbsent(MetricName.ACK_SUCCESS_TOTAL, name -> meter.counterBuilder(name.getName()).build());
    }

    LongCounter getAckFailureTotalCounter() {
        return counterMap.computeIfAbsent(MetricName.ACK_FAILURE_TOTAL, name -> meter.counterBuilder(name.getName()).build());
    }

    LongCounter getChangeInvisibleDurationSuccessCounter() {
        return counterMap.computeIfAbsent(MetricName.CHANGE_INVISIBLE_DURATION_SUCCESS_TOTAL, name -> meter.counterBuilder(name.getName()).build());
    }

    LongCounter getChangeInvisibleDurationFailureCounter() {
        return counterMap.computeIfAbsent(MetricName.CHANGE_INVISIBLE_DURATION_FAILURE_TOTAL, name -> meter.counterBuilder(name.getName()).build());
    }

    DoubleHistogram getMessageDeliveryLatencyHistogram() {
        return histogramMap.computeIfAbsent(MetricName.DELIVERY_LATENCY, name -> meter.histogramBuilder(name.getName()).build());
    }

    DoubleHistogram getMessageAwaitTimeHistogram() {
        return histogramMap.computeIfAbsent(MetricName.AWAIT_TIME, name -> meter.histogramBuilder(name.getName()).build());
    }

    @SuppressWarnings("deprecation")
    public synchronized void refresh(Metric metric) {
        try {
            if (!metric.isOn()) {
                shutdown();
            }
            final Optional<Endpoints> optionalEndpoints = metric.tryGetMetricEndpoints();
            if (!optionalEndpoints.isPresent()) {
                LOGGER.error("[Bug] Metric switch is on but endpoints is not filled, clientId={}", client.getClientId());
                return;
            }
            final Endpoints newMetricEndpoints = optionalEndpoints.get();
            if (newMetricEndpoints.equals(metricEndpoints)) {
                LOGGER.debug("Message metric exporter endpoints remains the same, clientId={}, endpoints={}", client.getClientId(), newMetricEndpoints);
                return;
            }
            final SslContext sslContext = GrpcSslContexts.forClient().trustManager(InsecureTrustManagerFactory.INSTANCE)
                .build();
            final NettyChannelBuilder channelBuilder =
                NettyChannelBuilder.forTarget(newMetricEndpoints.getGrpcTarget())
                    .sslContext(sslContext)
                    .intercept(new AuthInterceptor(client.getClientConfiguration(), client.getClientId()));
            final List<InetSocketAddress> socketAddresses = newMetricEndpoints.toSocketAddresses();
            if (null != socketAddresses) {
                IpNameResolverFactory metricResolverFactory = new IpNameResolverFactory(socketAddresses);
                channelBuilder.nameResolverFactory(metricResolverFactory);
            }
            ManagedChannel channel = channelBuilder.build();

            OtlpGrpcMetricExporter exporter = OtlpGrpcMetricExporter.builder()
                .setChannel(channel)
                .setTimeout(METRIC_EXPORTER_RPC_TIMEOUT).build();

            InstrumentSelector instrumentSelector = InstrumentSelector.builder()
                .setType(InstrumentType.HISTOGRAM)
                .setName("lingchuhistogram")
                .build();

            final View view = View.builder()
                .setAggregation(Aggregation.explicitBucketHistogram(Arrays.asList(0.1d, 1d, 10d, 30d, 60d)))
                .build();

            PeriodicMetricReader reader = PeriodicMetricReader.builder(exporter).setInterval(Duration.ofMinutes(1)).build();
            provider = SdkMeterProvider.builder().registerMetricReader(reader)
                .registerView(instrumentSelector, view)
                .build();
            final OpenTelemetrySdk openTelemetry = OpenTelemetrySdk.builder().setMeterProvider(provider).build();
            meter = openTelemetry.getMeter(METRIC_INSTRUMENTATION_NAME);
            LOGGER.info("Message meter exporter is updated, clientId={}, {} => {}", client.getClientId(), metricEndpoints, newMetricEndpoints);
            this.reset();
            metricEndpoints = newMetricEndpoints;
        } catch (Throwable t) {
            LOGGER.error("Exception raised while refreshing message meter, clientId={}", client.getClientId(), t);
        }
    }

    Optional<String> tryGetConsumerGroup() {
        final ClientImpl client = this.getClient();
        if (client instanceof PushConsumer) {
            return Optional.of(((PushConsumer) client).getConsumerGroup());
        }
        if (client instanceof SimpleConsumer) {
            return Optional.of(((SimpleConsumer) client).getConsumerGroup());
        }
        return Optional.empty();
    }

    public void shutdown() {
        LOGGER.info("Begin to shutdown the message meter, clientId={}", client.getClientId());
        if (null != provider) {
            final CountDownLatch latch = new CountDownLatch(1);
            provider.shutdown().whenComplete(latch::countDown);
            try {
                latch.await();
            } catch (Throwable t) {
                LOGGER.error("Exception raised while waiting for the shutdown of meter, clientId={}", client.getClientId());
            }
        }
        LOGGER.info("Shutdown the message meter, clientId={}", client.getClientId());
    }

    public Meter getMeter() {
        return meter;
    }

    public ClientImpl getClient() {
        return client;
    }

    private void reset() {
        counterMap.clear();
        histogramMap.clear();
        meter.gaugeBuilder(MetricName.CONSUMER_CACHED_MESSAGES.getName()).buildWithCallback(measurement -> {
            final Optional<String> optionalConsumerGroup = tryGetConsumerGroup();
            if (!optionalConsumerGroup.isPresent()) {
                // Skip because client is not push consumer.
                return;
            }
            final String consumerGroup = optionalConsumerGroup.get();
            final Map<String, Long> cachedMessageCountMap = messageCacheObserver.getCachedMessageCount();
            for (Map.Entry<String, Long> entry : cachedMessageCountMap.entrySet()) {
                final String topic = entry.getKey();
                Attributes attributes = Attributes.builder().put(RocketmqAttributes.TOPIC, topic)
                    .put(RocketmqAttributes.CONSUMER_GROUP, consumerGroup)
                    .put(RocketmqAttributes.CLIENT_ID, client.getClientId()).build();
                measurement.record(entry.getValue(), attributes);
            }
        });
        meter.gaugeBuilder(MetricName.CONSUMER_CACHED_BYTES.getName()).buildWithCallback(measurement -> {
            final Optional<String> optionalConsumerGroup = tryGetConsumerGroup();
            if (!optionalConsumerGroup.isPresent()) {
                // Skip because client is not push consumer.
                return;
            }
            final String consumerGroup = optionalConsumerGroup.get();
            final Map<String, Long> cachedMessageBytesMap = messageCacheObserver.getCachedMessageBytes();
            for (Map.Entry<String, Long> entry : cachedMessageBytesMap.entrySet()) {
                final String topic = entry.getKey();
                Attributes attributes = Attributes.builder().put(RocketmqAttributes.TOPIC, topic)
                    .put(RocketmqAttributes.CONSUMER_GROUP, consumerGroup)
                    .put(RocketmqAttributes.CLIENT_ID, client.getClientId()).build();
                measurement.record(entry.getValue(), attributes);
            }
        });
    }
}
