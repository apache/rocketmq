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
package org.apache.rocketmq.snode.service.impl;

import io.prometheus.client.Counter;
import io.prometheus.client.Summary;
import io.prometheus.client.exporter.HTTPServer;
import io.prometheus.client.hotspot.DefaultExports;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.utils.CallSnapshot;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.snode.exception.SnodeException;
import org.apache.rocketmq.snode.service.MetricsService;

public class MetricsServiceImpl implements MetricsService {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.SNODE_LOGGER_NAME);

    private HTTPServer server;

    private final Map<Integer, LinkedList<CallSnapshot>> requestTimesList = new ConcurrentHashMap<>();

    private final Map<String, LinkedList<CallSnapshot>> topicSizeList = new ConcurrentHashMap<>();

//    private Map<Integer, AtomicLong> requestTotalMap = new ConcurrentHashMap<>(512);
//
//    private Map<Integer, AtomicLong> requestFailedTotalMap = new ConcurrentHashMap<>(512);
//
//    private Map<String, AtomicLong> produceSizeTotal = new ConcurrentHashMap<>(512);
//
//    private Map<String, AtomicLong> consumeSizeTotal = new ConcurrentHashMap<>(512);

    private final Counter requestTotal = Counter.build().name("request_total").help("request total count").labelNames("requestCode").register();

    private final Counter requestFailedTotal = Counter.build().name("request_failed_total").help("request total count").labelNames("requestCode").register();

    private final Summary requestLatency = Summary.build()
        .quantile(0.5, 0.05)
        .quantile(0.9, 0.01)
        .quantile(0.99, 0.001)
        .name("requests_latency_seconds").labelNames("requestCode").help("Request latency in seconds.").register();

    private final Summary receivedBytes = Summary.build()
        .quantile(0.5, 0.05)
        .quantile(0.9, 0.01)
        .quantile(0.99, 0.001)
        .labelNames("topic")
        .name("sent_topic_size_bytes").help("Request size in bytes.").register();

//    public Double getLabelsValue(String labelValue) {
//        return CollectorRegistry.defaultRegistry.getSampleValue("request_total", new String[] {"requestCode"}, new String[] {labelValue});
//    }

    private AtomicLong getValue(ConcurrentHashMap map, Integer key) {
        AtomicLong value = (AtomicLong) map.get(key);
        if (value == null) {
            value = (AtomicLong) map.putIfAbsent(key, new AtomicLong(0));
        }
        return value;
    }

    @Override
    synchronized public void incRequestCount(int requestCode, boolean success) {
        if (!success) {
            this.requestFailedTotal.labels(requestCode + "").inc();
        } else {
            this.requestTotal.labels(requestCode + "").inc();
        }
    }

    @Override
    synchronized public void  recordRequestSize(String topic, double size) {
        this.receivedBytes.labels(topic).observe(size);
    }

    @Override
    public Timer startTimer(int requestCode) {
        return new PrometheusTimer(this.requestLatency).startTimer(requestCode);
    }

    @Override
    public void recordRequestLatency(Timer timer) {
        timer.observeDuration();
    }

    @Override public void start(int port) {
        try {
            DefaultExports.initialize();
            server = new HTTPServer(port);
        } catch (Exception ex) {
            log.error("Start metrics http server failed!", ex);
            throw new SnodeException(ResponseCode.SYSTEM_ERROR, "Start metrics http server failed!");
        }
    }

    @Override public void shutdown() {
        this.server.stop();
    }

    class PrometheusTimer implements Timer {

        private Summary.Timer timer;

        private Summary summary;

        public PrometheusTimer(Summary summary) {
            this.summary = summary;
        }

        @Override
        public Timer startTimer(int requestCode) {
            this.timer = summary.labels(requestCode + "").startTimer();
            return this;
        }

        @Override
        public void observeDuration() {
            if (this.timer != null) {
                this.timer.observeDuration();
            }
        }
    }

}
