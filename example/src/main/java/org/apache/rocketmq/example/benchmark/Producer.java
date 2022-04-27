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
package org.apache.rocketmq.example.benchmark;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.LongAdder;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.SerializeType;
import org.apache.rocketmq.srvutil.ServerUtil;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.Random;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicLong;

public class Producer {

    private static byte[] msgBody;
    private static final int MAX_LENGTH_ASYNC_QUEUE = 10000;
    private static final int SLEEP_FOR_A_WHILE = 100;

    public static void main(String[] args) throws MQClientException {
        System.setProperty(RemotingCommand.SERIALIZE_TYPE_PROPERTY, SerializeType.ROCKETMQ.name());

        Options options = ServerUtil.buildCommandlineOptions(new Options());
        CommandLine commandLine = ServerUtil.parseCmdLine("benchmarkProducer", args, buildCommandlineOptions(options), new PosixParser());
        if (null == commandLine) {
            System.exit(-1);
        }

        final String topic = commandLine.hasOption('t') ? commandLine.getOptionValue('t').trim() : "BenchmarkTest";
        final int messageSize = commandLine.hasOption('s') ? Integer.parseInt(commandLine.getOptionValue('s')) : 128;
        final boolean keyEnable = commandLine.hasOption('k') && Boolean.parseBoolean(commandLine.getOptionValue('k'));
        final int propertySize = commandLine.hasOption('p') ? Integer.parseInt(commandLine.getOptionValue('p')) : 0;
        final int tagCount = commandLine.hasOption('l') ? Integer.parseInt(commandLine.getOptionValue('l')) : 0;
        final boolean msgTraceEnable = commandLine.hasOption('m') && Boolean.parseBoolean(commandLine.getOptionValue('m'));
        final boolean aclEnable = commandLine.hasOption('a') && Boolean.parseBoolean(commandLine.getOptionValue('a'));
        final long messageNum = commandLine.hasOption('q') ? Long.parseLong(commandLine.getOptionValue('q')) : 0;
        final boolean delayEnable = commandLine.hasOption('d') && Boolean.parseBoolean(commandLine.getOptionValue('d'));
        final int delayLevel = commandLine.hasOption('e') ? Integer.parseInt(commandLine.getOptionValue('e')) : 1;
        final boolean asyncEnable = commandLine.hasOption('y') && Boolean.parseBoolean(commandLine.getOptionValue('y'));
        final int threadCount = asyncEnable ? 1 : commandLine.hasOption('w') ? Integer.parseInt(commandLine.getOptionValue('w')) : 64;

        System.out.printf("topic: %s threadCount: %d messageSize: %d keyEnable: %s propertySize: %d tagCount: %d " +
                "traceEnable: %s aclEnable: %s messageQuantity: %d%ndelayEnable: %s delayLevel: %s%n" +
                "asyncEnable: %s%n",
            topic, threadCount, messageSize, keyEnable, propertySize, tagCount, msgTraceEnable, aclEnable, messageNum,
            delayEnable, delayLevel, asyncEnable);

        StringBuilder sb = new StringBuilder(messageSize);
        for (int i = 0; i < messageSize; i++) {
            sb.append(RandomStringUtils.randomAlphanumeric(1));
        }
        msgBody = sb.toString().getBytes(StandardCharsets.UTF_8);

        final InternalLogger log = ClientLogger.getLog();

        final ExecutorService sendThreadPool = Executors.newFixedThreadPool(threadCount);

        final StatsBenchmarkProducer statsBenchmark = new StatsBenchmarkProducer();

        ScheduledExecutorService executorService = new ScheduledThreadPoolExecutor(1,
                new BasicThreadFactory.Builder().namingPattern("BenchmarkTimerThread-%d").daemon(true).build());

        final LinkedList<Long[]> snapshotList = new LinkedList<Long[]>();

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
            private void printStats() {
                if (snapshotList.size() >= 10) {
                    doPrintStats(snapshotList,  statsBenchmark, false);
                }
            }

            @Override
            public void run() {
                try {
                    this.printStats();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }, 10000, 10000, TimeUnit.MILLISECONDS);

        RPCHook rpcHook = null;
        if (aclEnable) {
            String ak = commandLine.hasOption("ak") ? String.valueOf(commandLine.getOptionValue("ak")) : AclClient.ACL_ACCESS_KEY;
            String sk = commandLine.hasOption("sk") ? String.valueOf(commandLine.getOptionValue("sk")) : AclClient.ACL_SECRET_KEY;
            rpcHook = AclClient.getAclRPCHook(ak, sk);
        }
        final DefaultMQProducer producer = new DefaultMQProducer("benchmark_producer", rpcHook, msgTraceEnable, null);
        producer.setInstanceName(Long.toString(System.currentTimeMillis()));

        if (commandLine.hasOption('n')) {
            String ns = commandLine.getOptionValue('n');
            producer.setNamesrvAddr(ns);
        }

        producer.setCompressMsgBodyOverHowmuch(Integer.MAX_VALUE);

        producer.start();

        for (int i = 0; i < threadCount; i++) {
            final long msgNumLimit = msgNums[i];
            if (messageNum > 0 && msgNumLimit == 0) {
                break;
            }
            sendThreadPool.execute(new Runnable() {
                @Override
                public void run() {
                    int num = 0;
                    while (true) {
                        try {
                            final Message msg = buildMessage(topic);
                            final long beginTimestamp = System.currentTimeMillis();
                            if (keyEnable) {
                                msg.setKeys(String.valueOf(beginTimestamp / 1000));
                            }
                            if (delayEnable) {
                                msg.setDelayTimeLevel(delayLevel);
                            }
                            if (tagCount > 0) {
                                msg.setTags(String.format("tag%d", System.currentTimeMillis() % tagCount));
                            }
                            if (propertySize > 0) {
                                if (msg.getProperties() != null) {
                                    msg.getProperties().clear();
                                }
                                int i = 0;
                                int startValue = (new Random(System.currentTimeMillis())).nextInt(100);
                                int size = 0;
                                while (true) {
                                    String prop1 = "prop" + i, prop1V = "hello" + startValue;
                                    String prop2 = "prop" + (i + 1), prop2V = String.valueOf(startValue);
                                    msg.putUserProperty(prop1, prop1V);
                                    msg.putUserProperty(prop2, prop2V);
                                    size += prop1.length() + prop2.length() + prop1V.length() + prop2V.length();
                                    if (size > propertySize) {
                                        break;
                                    }
                                    i += 2;
                                    startValue += 2;
                                }
                            }
                            if (asyncEnable) {
                                ThreadPoolExecutor e = (ThreadPoolExecutor) producer.getDefaultMQProducerImpl().getAsyncSenderExecutor();
                                // Flow control
                                while (e.getQueue().size() > MAX_LENGTH_ASYNC_QUEUE) {
                                    Thread.sleep(SLEEP_FOR_A_WHILE);
                                }
                                producer.send(msg, new SendCallback() {
                                    @Override
                                    public void onSuccess(SendResult sendResult) {
                                        updateStatsSuccess(statsBenchmark, beginTimestamp);
                                    }

                                    @Override
                                    public void onException(Throwable e) {
                                        statsBenchmark.getSendRequestFailedCount().increment();
                                    }
                                });
                            } else {
                                producer.send(msg);
                                updateStatsSuccess(statsBenchmark, beginTimestamp);
                            }
                        } catch (RemotingException e) {
                            statsBenchmark.getSendRequestFailedCount().increment();
                            log.error("[BENCHMARK_PRODUCER] Send Exception", e);

                            try {
                                Thread.sleep(3000);
                            } catch (InterruptedException ignored) {
                            }
                        } catch (InterruptedException e) {
                            statsBenchmark.getSendRequestFailedCount().increment();
                            try {
                                Thread.sleep(3000);
                            } catch (InterruptedException e1) {
                            }
                        } catch (MQClientException e) {
                            statsBenchmark.getSendRequestFailedCount().increment();
                            log.error("[BENCHMARK_PRODUCER] Send Exception", e);
                        } catch (MQBrokerException e) {
                            statsBenchmark.getReceiveResponseFailedCount().increment();
                            log.error("[BENCHMARK_PRODUCER] Send Exception", e);
                            try {
                                Thread.sleep(3000);
                            } catch (InterruptedException ignored) {
                            }
                        }
                        if (messageNum > 0 && ++num >= msgNumLimit) {
                            break;
                        }
                    }
                }
            });
        }
        try {
            sendThreadPool.shutdown();
            sendThreadPool.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
            executorService.shutdown();
            try {
                executorService.awaitTermination(5000, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
            }

            if (snapshotList.size() > 1) {
                doPrintStats(snapshotList, statsBenchmark, true);
            } else {
                System.out.printf("[Complete] Send Total: %d Send Failed: %d Response Failed: %d%n",
                    statsBenchmark.getSendRequestSuccessCount().longValue() + statsBenchmark.getSendRequestFailedCount().longValue(),
                    statsBenchmark.getSendRequestFailedCount().longValue(), statsBenchmark.getReceiveResponseFailedCount().longValue());
            }
            producer.shutdown();
        } catch (InterruptedException e) {
            log.error("[Exit] Thread Interrupted Exception", e);
        }
    }

    private static void updateStatsSuccess(StatsBenchmarkProducer statsBenchmark, long beginTimestamp) {
        statsBenchmark.getSendRequestSuccessCount().increment();
        statsBenchmark.getReceiveResponseSuccessCount().increment();
        final long currentRT = System.currentTimeMillis() - beginTimestamp;
        statsBenchmark.getSendMessageSuccessTimeTotal().add(currentRT);
        long prevMaxRT = statsBenchmark.getSendMessageMaxRT().longValue();
        while (currentRT > prevMaxRT) {
            boolean updated = statsBenchmark.getSendMessageMaxRT().compareAndSet(prevMaxRT, currentRT);
            if (updated)
                break;

            prevMaxRT = statsBenchmark.getSendMessageMaxRT().longValue();
        }
    }

    public static Options buildCommandlineOptions(final Options options) {
        Option opt = new Option("w", "threadCount", true, "Thread count, Default: 64");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("s", "messageSize", true, "Message Size, Default: 128");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("k", "keyEnable", true, "Message Key Enable, Default: false");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("t", "topic", true, "Topic name, Default: BenchmarkTest");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("l", "tagCount", true, "Tag count, Default: 0");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("m", "msgTraceEnable", true, "Message Trace Enable, Default: false");
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

        opt = new Option("q", "messageQuantity", true, "Send message quantity, Default: 0, running forever");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("d", "delayEnable", true, "Delay message Enable, Default: false");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("e", "delayLevel", true, "Delay message level, Default: 1");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("y", "asyncEnable", true, "Enable async produce, Default: false");
        opt.setRequired(false);
        options.addOption(opt);

        return options;
    }

    private static Message buildMessage(final String topic) {
        return new Message(topic, msgBody);
    }

    private static void doPrintStats(final LinkedList<Long[]> snapshotList, final StatsBenchmarkProducer statsBenchmark, boolean done) {
        Long[] begin = snapshotList.getFirst();
        Long[] end = snapshotList.getLast();

        final long sendTps = (long) (((end[3] - begin[3]) / (double) (end[0] - begin[0])) * 1000L);
        final double averageRT = (end[5] - begin[5]) / (double) (end[3] - begin[3]);

        if (done) {
            System.out.printf("[Complete] Send Total: %d Send TPS: %d Max RT(ms): %d Average RT(ms): %7.3f Send Failed: %d Response Failed: %d%n",
                statsBenchmark.getSendRequestSuccessCount().longValue() + statsBenchmark.getSendRequestFailedCount().longValue(),
                sendTps, statsBenchmark.getSendMessageMaxRT().longValue(), averageRT, end[2], end[4]);
        } else {
            System.out.printf("Current Time: %s Send TPS: %d Max RT(ms): %d Average RT(ms): %7.3f Send Failed: %d Response Failed: %d%n",
                UtilAll.timeMillisToHumanString2(System.currentTimeMillis()), sendTps, statsBenchmark.getSendMessageMaxRT().longValue(), averageRT, end[2], end[4]);
        }
    }
}

class StatsBenchmarkProducer {
    private final LongAdder sendRequestSuccessCount = new LongAdder();

    private final LongAdder sendRequestFailedCount = new LongAdder();

    private final LongAdder receiveResponseSuccessCount = new LongAdder();

    private final LongAdder receiveResponseFailedCount = new LongAdder();

    private final LongAdder sendMessageSuccessTimeTotal = new LongAdder();

    private final AtomicLong sendMessageMaxRT = new AtomicLong(0L);

    public Long[] createSnapshot() {
        Long[] snap = new Long[] {
            System.currentTimeMillis(),
            this.sendRequestSuccessCount.longValue(),
            this.sendRequestFailedCount.longValue(),
            this.receiveResponseSuccessCount.longValue(),
            this.receiveResponseFailedCount.longValue(),
            this.sendMessageSuccessTimeTotal.longValue(),
        };

        return snap;
    }

    public LongAdder getSendRequestSuccessCount() {
        return sendRequestSuccessCount;
    }

    public LongAdder getSendRequestFailedCount() {
        return sendRequestFailedCount;
    }

    public LongAdder getReceiveResponseSuccessCount() {
        return receiveResponseSuccessCount;
    }

    public LongAdder getReceiveResponseFailedCount() {
        return receiveResponseFailedCount;
    }

    public LongAdder getSendMessageSuccessTimeTotal() {
        return sendMessageSuccessTimeTotal;
    }

    public AtomicLong getSendMessageMaxRT() {
        return sendMessageMaxRT;
    }
}
