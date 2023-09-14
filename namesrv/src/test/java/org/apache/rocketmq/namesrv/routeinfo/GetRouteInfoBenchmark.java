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

import io.netty.channel.Channel;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.namesrv.NamesrvConfig;
import org.apache.rocketmq.common.utils.ThreadUtils;
import org.apache.rocketmq.remoting.protocol.DataVersion;
import org.apache.rocketmq.remoting.protocol.body.TopicConfigSerializeWrapper;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import static org.mockito.Mockito.mock;

@BenchmarkMode(Mode.SampleTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
public class GetRouteInfoBenchmark {
    private RouteInfoManager routeInfoManager;
    private String[] topicList = new String[40000];
    private ExecutorService es = Executors.newCachedThreadPool();

    @Setup
    public void setup() throws InterruptedException {

        routeInfoManager = new RouteInfoManager(new NamesrvConfig(), null);

        // Init 4 clusters and 8 brokers in each cluster
        // Each cluster has 10000 topics

        for (int i = 0; i < 40000; i++) {
            final String topic = RandomStringUtils.randomAlphabetic(32) + i;
            topicList[i] = topic;
        }

        for (int i = 0; i < 4; i++) {
            // Cluster iteration
            final String clusterName = "Default-Cluster-" + i;
            for (int j = 0; j < 8; j++) {
                // broker iteration
                final int startTopicIndex = i * 10000;
                final String brokerName = "Default-Broker-" + j;
                final String brokerAddr = "127.0.0.1:500" + i * j;
                es.submit(new Runnable() {
                    @Override
                    public void run() {
                        DataVersion dataVersion = new DataVersion();
                        dataVersion.setCounter(new AtomicLong(10L));
                        dataVersion.setTimestamp(100L);

                        ConcurrentHashMap<String, TopicConfig> topicConfigConcurrentHashMap = new ConcurrentHashMap<>();

                        for (int k = startTopicIndex; k < startTopicIndex + 10000; k++) {
                            TopicConfig topicConfig = new TopicConfig();
                            topicConfig.setWriteQueueNums(8);
                            topicConfig.setTopicName(topicList[k]);
                            topicConfig.setPerm(6);
                            topicConfig.setReadQueueNums(8);
                            topicConfig.setOrder(false);
                            topicConfigConcurrentHashMap.put(topicList[k], topicConfig);
                        }

                        while (true) {
                            try {
                                TimeUnit.MILLISECONDS.sleep(new Random().nextInt(100));
                            } catch (InterruptedException ignored) {
                            }

                            dataVersion.nextVersion();
                            TopicConfigSerializeWrapper topicConfigSerializeWrapper = new TopicConfigSerializeWrapper();
                            topicConfigSerializeWrapper.setDataVersion(dataVersion);
                            topicConfigSerializeWrapper.setTopicConfigTable(topicConfigConcurrentHashMap);
                            Channel channel = mock(Channel.class);

                            routeInfoManager.registerBroker(clusterName, brokerAddr, brokerName, 0, brokerAddr, "",
                                null, topicConfigSerializeWrapper, new ArrayList<>(), channel);
                        }
                    }
                });
            }
        }

        // Wait threads startup
        TimeUnit.SECONDS.sleep(3);
    }

    @TearDown
    public void tearDown() {
        ThreadUtils.shutdownGracefully(es, 3, TimeUnit.SECONDS);
    }

    @Benchmark
    @Fork(value = 2)
    @Measurement(iterations = 10, time = 10)
    @Warmup(iterations = 10, time = 1)
    @Threads(4) // Assume we have 128 clients try to pick up route data concurrently
    public void pickupTopicRouteData() {
        routeInfoManager.pickupTopicRouteData(topicList[new Random().nextInt(40000)]);
    }

    public static void main(String[] args) throws Exception {
        org.openjdk.jmh.Main.main(args);
    }
}
