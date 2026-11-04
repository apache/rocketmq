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

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.TimerTask;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientConfigurationBuilder;
import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.client.apis.SessionCredentialsProvider;
import org.apache.rocketmq.client.apis.StaticSessionCredentialsProvider;
import org.apache.rocketmq.client.apis.message.Message;
import org.apache.rocketmq.client.apis.message.MessageBuilder;
import org.apache.rocketmq.client.apis.producer.Producer;
import org.apache.rocketmq.common.UtilAll;

public class GrpcProducer {

    private static byte[] msgBody;

    public static void main(String[] args) throws Exception {
        Options options = buildCommandlineOptions();
        CommandLine commandLine = new DefaultParser().parse(options, args);
        if (null == commandLine) {
            System.exit(-1);
        }

        if (commandLine.hasOption('h')) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("GrpcProducer", options, true);
            System.exit(0);
        }

        final String endpoint = commandLine.hasOption('n') ? commandLine.getOptionValue('n').trim() : "127.0.0.1:8081";
        final String topic = commandLine.hasOption('t') ? commandLine.getOptionValue('t').trim() : "BenchmarkTest";
        final int threadCount = commandLine.hasOption('w') ? Integer.parseInt(commandLine.getOptionValue('w')) : 64;
        final int messageSize = commandLine.hasOption('s') ? Integer.parseInt(commandLine.getOptionValue('s')) : 128;
        final boolean keyEnable = commandLine.hasOption('k') && Boolean.parseBoolean(commandLine.getOptionValue('k'));
        final long messageNum = commandLine.hasOption('q') ? Long.parseLong(commandLine.getOptionValue('q')) : 0;
        final boolean aclEnable = commandLine.hasOption('a') && Boolean.parseBoolean(commandLine.getOptionValue('a'));
        final String accessKey = commandLine.hasOption("ak") ? commandLine.getOptionValue("ak") : "12345678";
        final String secretKey = commandLine.hasOption("sk") ? commandLine.getOptionValue("sk") : "rocketmq2";
        final boolean sslEnable = commandLine.hasOption("ssl") && Boolean.parseBoolean(commandLine.getOptionValue("ssl"));
        final int reportInterval = commandLine.hasOption("ri") ? Integer.parseInt(commandLine.getOptionValue("ri")) : 10000;

        System.out.printf("endpoint: %s, topic: %s, threadCount: %d, messageSize: %d, keyEnable: %s, "
                + "messageQuantity: %d, aclEnable: %s, sslEnable: %s, reportInterval: %d%n",
            endpoint, topic, threadCount, messageSize, keyEnable, messageNum, aclEnable, sslEnable, reportInterval);

        StringBuilder sb = new StringBuilder(messageSize);
        for (int i = 0; i < messageSize; i++) {
            sb.append(RandomStringUtils.randomAlphanumeric(1));
        }
        msgBody = sb.toString().getBytes(StandardCharsets.UTF_8);

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

        final Producer producer = provider.newProducerBuilder()
            .setClientConfiguration(clientConfig)
            .setTopics(topic)
            .build();

        final StatsBenchmarkGrpcProducer statsBenchmark = new StatsBenchmarkGrpcProducer();
        final LinkedList<Long[]> snapshotList = new LinkedList<>();

        final ExecutorService sendThreadPool = Executors.newFixedThreadPool(threadCount);

        ScheduledExecutorService executorService = new ScheduledThreadPoolExecutor(1,
            new BasicThreadFactory.Builder().namingPattern("BenchmarkTimerThread-%d").daemon(true).build());

        final long[] msgNums = new long[threadCount];
        if (messageNum > 0) {
            Arrays.fill(msgNums, messageNum / threadCount);
            long mod = messageNum % threadCount;
            if (mod > 0) {
                msgNums[0] += mod;
            }
        }

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
                        doPrintStats(snapshotList, statsBenchmark, false);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }, reportInterval, reportInterval, TimeUnit.MILLISECONDS);

        for (int i = 0; i < threadCount; i++) {
            final long msgNumLimit = msgNums[i];
            if (messageNum > 0 && msgNumLimit == 0) {
                break;
            }
            sendThreadPool.execute(() -> {
                int num = 0;
                while (true) {
                    try {
                        final long beginTimestamp = System.currentTimeMillis();
                        MessageBuilder msgBuilder = provider.newMessageBuilder()
                            .setTopic(topic)
                            .setBody(msgBody);
                        if (keyEnable) {
                            msgBuilder.setKeys(String.valueOf(beginTimestamp / 1000));
                        }
                        Message msg = msgBuilder.build();

                        producer.send(msg);
                        updateStatsSuccess(statsBenchmark, beginTimestamp);
                    } catch (Exception e) {
                        statsBenchmark.getSendFailedCount().increment();
                        System.err.println("[BENCHMARK_PRODUCER] Send Exception: " + e.getMessage());
                        e.printStackTrace(System.err);
                        try {
                            Thread.sleep(3000);
                        } catch (InterruptedException ignored) {
                        }
                    }
                    if (messageNum > 0 && ++num >= msgNumLimit) {
                        break;
                    }
                }
            });
        }

        try {
            sendThreadPool.shutdown();
            sendThreadPool.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
            executorService.shutdown();
            executorService.awaitTermination(5000, TimeUnit.MILLISECONDS);

            if (snapshotList.size() > 1) {
                doPrintStats(snapshotList, statsBenchmark, true);
            } else {
                System.out.printf("[Complete] Send Total: %d Send Failed: %d%n",
                    statsBenchmark.getSendSuccessCount().longValue() + statsBenchmark.getSendFailedCount().longValue(),
                    statsBenchmark.getSendFailedCount().longValue());
            }
            producer.close();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static void updateStatsSuccess(StatsBenchmarkGrpcProducer statsBenchmark, long beginTimestamp) {
        statsBenchmark.getSendSuccessCount().increment();
        final long currentRT = System.currentTimeMillis() - beginTimestamp;
        statsBenchmark.getSendTimeTotal().add(currentRT);
        long prevMaxRT = statsBenchmark.getSendMaxRT().longValue();
        while (currentRT > prevMaxRT) {
            boolean updated = statsBenchmark.getSendMaxRT().compareAndSet(prevMaxRT, currentRT);
            if (updated) break;
            prevMaxRT = statsBenchmark.getSendMaxRT().longValue();
        }
    }

    private static void doPrintStats(LinkedList<Long[]> snapshotList, StatsBenchmarkGrpcProducer statsBenchmark, boolean done) {
        Long[] begin = snapshotList.getFirst();
        Long[] end = snapshotList.getLast();

        final long sendTps = (long) (((end[1] - begin[1]) / (double) (end[0] - begin[0])) * 1000L);
        final double averageRT = (end[3] - begin[3]) / (double) (end[1] - begin[1]);

        if (done) {
            System.out.printf("[Complete] Send Total: %d | Send TPS: %d | Max RT(ms): %d | Average RT(ms): %7.3f | Send Failed: %d%n",
                statsBenchmark.getSendSuccessCount().longValue() + statsBenchmark.getSendFailedCount().longValue(),
                sendTps, statsBenchmark.getSendMaxRT().longValue(), averageRT, end[2]);
        } else {
            System.out.printf("Current Time: %s | Send TPS: %d | Max RT(ms): %d | Average RT(ms): %7.3f | Send Failed: %d%n",
                UtilAll.timeMillisToHumanString2(System.currentTimeMillis()), sendTps, statsBenchmark.getSendMaxRT().longValue(), averageRT, end[2]);
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

        opt = new Option("w", "threadCount", true, "Thread count, Default: 64");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("s", "messageSize", true, "Message Size, Default: 128");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("k", "keyEnable", true, "Message Key Enable, Default: false");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("q", "messageQuantity", true, "Send message quantity, Default: 0, running forever");
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

class StatsBenchmarkGrpcProducer {
    private final LongAdder sendSuccessCount = new LongAdder();
    private final LongAdder sendFailedCount = new LongAdder();
    private final LongAdder sendTimeTotal = new LongAdder();
    private final AtomicLong sendMaxRT = new AtomicLong(0L);

    public Long[] createSnapshot() {
        return new Long[] {
            System.currentTimeMillis(),
            this.sendSuccessCount.longValue(),
            this.sendFailedCount.longValue(),
            this.sendTimeTotal.longValue(),
        };
    }

    public LongAdder getSendSuccessCount() {
        return sendSuccessCount;
    }

    public LongAdder getSendFailedCount() {
        return sendFailedCount;
    }

    public LongAdder getSendTimeTotal() {
        return sendTimeTotal;
    }

    public AtomicLong getSendMaxRT() {
        return sendMaxRT;
    }
}
