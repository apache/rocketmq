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
package org.apache.rocketmq.namesrv.routeinfo;

import com.alibaba.fastjson2.JSONWriter;
import io.netty.channel.Channel;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.namesrv.NamesrvConfig;
import org.apache.rocketmq.remoting.protocol.body.TopicConfigSerializeWrapper;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Group;
import org.openjdk.jmh.annotations.GroupThreads;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import static org.mockito.Mockito.mock;

/**
 * Compare cache disabled/enabled on the same workload, including concurrent registration.
 * Run with -prof gc to report allocation, and retain per-fork results for comparison.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(2)
public class TopicRouteCacheBenchmark {
    @State(Scope.Benchmark)
    public static class Routes {
        @Param({"0", "8388608"})
        public int cacheBytes;

        @Param({"1", "10000"})
        public int topics;

        @Param({"8"})
        public int brokers;

        public RouteInfoManager manager;
        public TopicConfigSerializeWrapper wrapper;
        public Channel channel;
        public String[] topicNames;
        private long registrationVersion;

        @Setup
        public void setup() {
            NamesrvConfig config = new NamesrvConfig();
            config.setRouteInfoCacheMaxBytes(cacheBytes);
            manager = new RouteInfoManager(config, null);
            channel = mock(Channel.class);
            wrapper = new TopicConfigSerializeWrapper();
            topicNames = new String[topics];
            for (int i = 0; i < topics; i++) {
                topicNames[i] = "benchmark-topic-" + i;
                wrapper.getTopicConfigTable().put(topicNames[i], new TopicConfig(topicNames[i], 8, 8));
            }
            // A one-topic initial registration is deliberately refused by RouteInfoManager.
            wrapper.getTopicConfigTable().put("registration-topic", new TopicConfig("registration-topic", 8, 8));
            for (int i = 0; i < brokers; i++) {
                manager.registerBroker("cluster", "address-" + i, "broker-" + i, 0, "ha",
                    "zone", null, wrapper, Collections.emptyList(), channel);
            }
            for (String topic : topicNames) {
                if (cacheBytes >= 0) {
                    manager.pickupTopicRouteDataEncoded(topic, null, true);
                }
            }
        }

        public void register() {
            TopicConfigSerializeWrapper registration = new TopicConfigSerializeWrapper();
            registration.setTopicConfigTable(wrapper.getTopicConfigTable());
            // Separate decoded versions force the normal topic comparison path, even
            // though effective routes are unchanged. Never mutate the live table's version.
            registration.getDataVersion().setCounter(new AtomicLong(++registrationVersion));
            manager.registerBroker("cluster", "address-0", "broker-0", 0, "ha",
                "zone", null, registration, Collections.emptyList(), channel);
        }
    }

    @State(Scope.Thread)
    public static class Cursor {
        private int next;

        String topic(Routes routes) {
            String topic = routes.topicNames[next++];
            if (next == routes.topics) {
                next = 0;
            }
            return topic;
        }
    }

    @Benchmark
    public byte[] query(Routes routes, Cursor cursor) {
        if (routes.cacheBytes < 0) {
            // Allows the same benchmark class to run against unmodified upstream classes.
            return routes.manager.pickupTopicRouteData(cursor.topic(routes))
                .encode(JSONWriter.Feature.BrowserCompatible, JSONWriter.Feature.MapSortField);
        }
        return routes.manager.pickupTopicRouteDataEncoded(cursor.topic(routes), null, true);
    }

    @Benchmark
    public void registration(Routes routes) {
        routes.register();
    }

    @Benchmark
    @Group("mixed")
    @GroupThreads(4)
    public byte[] queryDuringRegistration(Routes routes, Cursor cursor) {
        return query(routes, cursor);
    }

    @Benchmark
    @Group("mixed")
    @GroupThreads(1)
    public void concurrentRegistration(Routes routes) {
        routes.register();
    }
}
