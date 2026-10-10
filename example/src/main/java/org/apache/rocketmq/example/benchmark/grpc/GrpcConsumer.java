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
package org.apache.rocketmq.example.benchmark.grpc;

import java.time.Duration;
import java.util.Collections;
import java.util.LinkedList;
import java.util.TimerTask;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientConfigurationBuilder;
import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.client.apis.SessionCredentialsProvider;
import org.apache.rocketmq.client.apis.StaticSessionCredentialsProvider;
import org.apache.rocketmq.client.apis.consumer.ConsumeResult;
import org.apache.rocketmq.client.apis.consumer.FilterExpression;
import org.apache.rocketmq.client.apis.consumer.PushConsumer;
import org.apache.rocketmq.client.apis.consumer.PushConsumerBuilder;
import org.apache.rocketmq.common.UtilAll;

public class GrpcConsumer {

    public static void main(String[] args) throws Exception {
        Options options = buildCommandlineOptions();
        CommandLine commandLine = new DefaultParser().parse(options, args);
        if (null == commandLine) {
            System.exit(-1);
        }

        if (commandLine.hasOption('h')) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("GrpcConsumer", options, true);
            System.exit(0);
        }

        final String endpoint = commandLine.hasOption('n') ? commandLine.getOptionValue('n').trim() : "127.0.0.1:8081";
        final String topic = commandLine.hasOption('t') ? commandLine.getOptionValue('t').trim() : "BenchmarkTest";
        final int threadCount = commandLine.hasOption('w') ? Integer.parseInt(commandLine.getOptionValue('w')) : 20;
        final String group = commandLine.hasOption('g') ? commandLine.getOptionValue('g').trim() : "benchmark_consumer";
        final int maxCacheMessageCount = commandLine.hasOption("mc") ? Integer.parseInt(commandLine.getOptionValue("mc")) : -1;
        final boolean aclEnable = commandLine.hasOption('a') && Boolean.parseBoolean(commandLine.getOptionValue('a'));
        final String accessKey = commandLine.hasOption("ak") ? commandLine.getOptionValue("ak") : "12345678";
        final String secretKey = commandLine.hasOption("sk") ? commandLine.getOptionValue("sk") : "rocketmq2";
        final boolean sslEnable = commandLine.hasOption("ssl") && Boolean.parseBoolean(commandLine.getOptionValue("ssl"));
        final int reportInterval = commandLine.hasOption("ri") ? Integer.parseInt(commandLine.getOptionValue("ri")) : 10000;

        System.out.printf("endpoint: %s, topic: %s, threadCount: %d, group: %s, maxCacheMessageCount: %d, "
                + "aclEnable: %s, sslEnable: %s, reportInterval: %d%n",
            endpoint, topic, threadCount, group, maxCacheMessageCount, aclEnable, sslEnable, reportInterval);

        final StatsBenchmarkGrpcConsumer statsBenchmark = new StatsBenchmarkGrpcConsumer();

        ScheduledExecutorService executorService = new ScheduledThreadPoolExecutor(1,
            new BasicThreadFactory.Builder().namingPattern("BenchmarkTimerThread-%d").daemon(true).build());

        final LinkedList<Long[]> snapshotList = new LinkedList<>();

        executorService.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                snapshotList.addLast(statsBenchmark.createSnapshot());
                if (snapshotList.size() > 10) {
                    snapshotList.removeFirst();
                }
            }
        }, 1000, 1000, TimeUnit.MILLISECONDS);

        executorService.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                try {
                    if (snapshotList.size() >= 10) {
                        Long[] begin = snapshotList.getFirst();
                        Long[] end = snapshotList.getLast();

                        final long consumeTps = (long) (((end[1] - begin[1]) / (double) (end[0] - begin[0])) * 1000L);
                        final double averageB2CRT = (end[2] - begin[2]) / (double) (end[1] - begin[1]);
                        final long b2cMax = statsBenchmark.getBorn2ConsumerMaxRT().get();

                        statsBenchmark.getBorn2ConsumerMaxRT().set(0);

                        System.out.printf("Current Time: %s | Consume TPS: %d | AVG(B2C) RT(ms): %7.3f | MAX(B2C) RT(ms): %d%n",
                            UtilAll.timeMillisToHumanString2(System.currentTimeMillis()), consumeTps, averageB2CRT, b2cMax);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }, reportInterval, reportInterval, TimeUnit.MILLISECONDS);

        final ClientServiceProvider provider = ClientServiceProvider.loadService();

        ClientConfigurationBuilder configBuilder = new ClientConfigurationBuilder()
            .setEndpoints(endpoint)
            .enableSsl(sslEnable)
            .setRequestTimeout(Duration.ofSeconds(10));

        if (aclEnable) {
            SessionCredentialsProvider credProvider = new StaticSessionCredentialsProvider(accessKey, secretKey);
            configBuilder.setCredentialProvider(credProvider);
        }

        ClientConfiguration clientConfig = configBuilder.build();

        PushConsumerBuilder consumerBuilder = provider.newPushConsumerBuilder()
            .setClientConfiguration(clientConfig)
            .setConsumerGroup(group)
            .setSubscriptionExpressions(Collections.singletonMap(topic, FilterExpression.SUB_ALL))
            .setConsumptionThreadCount(threadCount)
            .setMessageListener(messageView -> {
                long now = System.currentTimeMillis();
                statsBenchmark.getReceiveMessageTotalCount().increment();

                long born2ConsumerRT = now - messageView.getBornTimestamp();
                statsBenchmark.getBorn2ConsumerTotalRT().add(born2ConsumerRT);
                compareAndSetMax(statsBenchmark.getBorn2ConsumerMaxRT(), born2ConsumerRT);

                return ConsumeResult.SUCCESS;
            });

        if (maxCacheMessageCount > 0) {
            consumerBuilder.setMaxCacheMessageCount(maxCacheMessageCount);
        }

        PushConsumer consumer = consumerBuilder.build();

        System.out.printf("GrpcConsumer Started.%n");
    }

    private static void compareAndSetMax(AtomicLong target, long value) {
        long prev = target.get();
        while (value > prev) {
            boolean updated = target.compareAndSet(prev, value);
            if (updated) break;
            prev = target.get();
        }
    }

    private static Options buildCommandlineOptions() {
        Options options = new Options();

        Option opt = new Option("h", "help", false, "Print help");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("n", "endpoint", true, "Proxy gRPC endpoint, Default: 127.0.0.1:8081");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("t", "topic", true, "Topic name, Default: BenchmarkTest");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("w", "threadCount", true, "Consumption thread count, Default: 20");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("g", "group", true, "Consumer group name, Default: benchmark_consumer");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("mc", "maxCacheMessageCount", true, "Max cache message count, Default: client default");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("a", "aclEnable", true, "Acl Enable, Default: false");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("ak", "accessKey", true, "Acl access key, Default: 12345678");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("sk", "secretKey", true, "Acl secret key, Default: rocketmq2");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("ssl", "sslEnable", true, "Enable SSL/TLS, Default: false");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("ri", "reportInterval", true, "The number of ms between reports, Default: 10000");
        opt.setRequired(false);
        options.addOption(opt);

        return options;
    }
}

class StatsBenchmarkGrpcConsumer {
    private final LongAdder receiveMessageTotalCount = new LongAdder();
    private final LongAdder born2ConsumerTotalRT = new LongAdder();
    private final AtomicLong born2ConsumerMaxRT = new AtomicLong(0L);

    public Long[] createSnapshot() {
        return new Long[] {
            System.currentTimeMillis(),
            this.receiveMessageTotalCount.longValue(),
            this.born2ConsumerTotalRT.longValue(),
        };
    }

    public LongAdder getReceiveMessageTotalCount() {
        return receiveMessageTotalCount;
    }

    public LongAdder getBorn2ConsumerTotalRT() {
        return born2ConsumerTotalRT;
    }

    public AtomicLong getBorn2ConsumerMaxRT() {
        return born2ConsumerMaxRT;
    }
}
