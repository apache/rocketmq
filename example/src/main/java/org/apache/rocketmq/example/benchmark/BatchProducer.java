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
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.rocketmq.acl.common.AclClientRPCHook;
import org.apache.rocketmq.acl.common.SessionCredentials;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.SerializeType;
import org.apache.rocketmq.srvutil.ServerUtil;

public class BatchProducer {

    private static byte[] msgBody;

    public static void main(String[] args) throws MQClientException {
        System.setProperty(RemotingCommand.SERIALIZE_TYPE_PROPERTY, SerializeType.ROCKETMQ.name());

        Options options = ServerUtil.buildCommandlineOptions(new Options());
        CommandLine commandLine = ServerUtil.parseCmdLine("benchmarkBatchProducer", args, buildCommandlineOptions(options), new PosixParser());
        if (null == commandLine) {
            System.exit(-1);
        }

        final String namesrv = getOptionValue(commandLine, 'n', "127.0.0.1:9876");
        final String topic = getOptionValue(commandLine, 't', "BenchmarkTest");
        final int threadCount = getOptionValue(commandLine, 'w', 64);
        final int messageSize = getOptionValue(commandLine, 's', 128);
        final int batchSize = getOptionValue(commandLine, 'b', 16);
        final boolean keyEnable = getOptionValue(commandLine, 'k', false);
        final int propertySize = getOptionValue(commandLine, 'p', 0);
        final int tagCount = getOptionValue(commandLine, 'l', 0);
        final boolean msgTraceEnable = getOptionValue(commandLine, 'm', false);
        final boolean aclEnable = getOptionValue(commandLine, 'a', false);
        final String ak = getOptionValue(commandLine, 'c', "rocketmq2");
        final String sk = getOptionValue(commandLine, 'e', "12346789");

        System.out.printf("topic: %s threadCount: %d messageSize: %d batchSize: %d keyEnable: %s propertySize: %d tagCount: %d traceEnable: %s aclEnable: %s%n",
                topic, threadCount, messageSize, batchSize, keyEnable, propertySize, tagCount, msgTraceEnable, aclEnable);

        StringBuilder sb = new StringBuilder(messageSize);
        for (int i = 0; i < messageSize; i++) {
            sb.append(RandomStringUtils.randomAlphanumeric(1));
        }
        msgBody = sb.toString().getBytes(StandardCharsets.UTF_8);

        final StatsBenchmarkBatchProducer statsBenchmark = new StatsBenchmarkBatchProducer();
        statsBenchmark.start();

        final DefaultMQProducer producer = initInstance(namesrv, msgTraceEnable, aclEnable, ak, sk);
        producer.start();

        final InternalLogger log = ClientLogger.getLog();
        final ExecutorService sendThreadPool = Executors.newFixedThreadPool(threadCount);
        for (int i = 0; i < threadCount; i++) {
            sendThreadPool.execute(new Runnable() {
                @Override
                public void run() {
                    while (true) {
                        List<Message> msgs = buildBathMessage(batchSize, topic);

                        if (CollectionUtils.isEmpty(msgs)) {
                            return;
                        }

                        try {
                            long beginTimestamp = System.currentTimeMillis();
                            long sendSucCount = statsBenchmark.getSendMessageSuccessCount().longValue();

                            setKeys(keyEnable, msgs, String.valueOf(beginTimestamp / 1000));
                            setTags(tagCount, msgs, sendSucCount);
                            setProperties(propertySize, msgs);
                            SendResult sendResult = producer.send(msgs);
                            if (sendResult.getSendStatus() == SendStatus.SEND_OK) {
                                statsBenchmark.getSendRequestSuccessCount().increment();
                                statsBenchmark.getSendMessageSuccessCount().add(msgs.size());
                            } else {
                                statsBenchmark.getSendRequestFailedCount().increment();
                                statsBenchmark.getSendMessageFailedCount().add(msgs.size());
                            }
                            long currentRT = System.currentTimeMillis() - beginTimestamp;
                            statsBenchmark.getSendMessageSuccessTimeTotal().add(currentRT);
                            long prevMaxRT = statsBenchmark.getSendMessageMaxRT().longValue();
                            while (currentRT > prevMaxRT) {
                                boolean updated = statsBenchmark.getSendMessageMaxRT().compareAndSet(prevMaxRT, currentRT);
                                if (updated) {
                                    break;
                                }

                                prevMaxRT = statsBenchmark.getSendMessageMaxRT().get();
                            }
                        } catch (RemotingException e) {
                            statsBenchmark.getSendRequestFailedCount().increment();
                            statsBenchmark.getSendMessageFailedCount().add(msgs.size());
                            log.error("[BENCHMARK_PRODUCER] Send Exception", e);

                            try {
                                Thread.sleep(3000);
                            } catch (InterruptedException ignored) {
                            }
                        } catch (InterruptedException e) {
                            statsBenchmark.getSendRequestFailedCount().increment();
                            statsBenchmark.getSendMessageFailedCount().add(msgs.size());
                            try {
                                Thread.sleep(3000);
                            } catch (InterruptedException e1) {
                            }
                            statsBenchmark.getSendRequestFailedCount().increment();
                            statsBenchmark.getSendMessageFailedCount().add(msgs.size());
                            log.error("[BENCHMARK_PRODUCER] Send Exception", e);
                        } catch (MQClientException e) {
                            statsBenchmark.getSendRequestFailedCount().increment();
                            statsBenchmark.getSendMessageFailedCount().add(msgs.size());
                            log.error("[BENCHMARK_PRODUCER] Send Exception", e);
                        } catch (MQBrokerException e) {
                            statsBenchmark.getSendRequestFailedCount().increment();
                            statsBenchmark.getSendMessageFailedCount().add(msgs.size());
                            log.error("[BENCHMARK_PRODUCER] Send Exception", e);
                            try {
                                Thread.sleep(3000);
                            } catch (InterruptedException ignored) {
                            }
                        }
                    }
                }
            });
        }
    }

    public static Options buildCommandlineOptions(final Options options) {
        Option opt = new Option("w", "threadCount", true, "Thread count, Default: 64");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("s", "messageSize", true, "Message Size, Default: 128");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("b", "batchSize", true, "Batch Size, Default: 16");
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

        opt = new Option("c", "accessKey", true, "Acl Access Key, Default: rocketmq2");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("e", "secretKey", true, "Acl Secret Key, Default: 123456789");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("p", "propertySize", true, "Property Size, Default: 0");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("n", "namesrv", true, "name server, Default: 127.0.0.1:9876");
        opt.setRequired(false);
        options.addOption(opt);
        return options;
    }

    private static String getOptionValue(CommandLine commandLine, char key, String defaultValue) {
        if (commandLine.hasOption(key)) {
            return commandLine.getOptionValue(key).trim();
        }
        return defaultValue;
    }

    private static int getOptionValue(CommandLine commandLine, char key, int defaultValue) {
        if (commandLine.hasOption(key)) {
            return Integer.parseInt(commandLine.getOptionValue(key).trim());
        }
        return defaultValue;
    }

    private static boolean getOptionValue(CommandLine commandLine, char key, boolean defaultValue) {
        if (commandLine.hasOption(key)) {
            return Boolean.parseBoolean(commandLine.getOptionValue(key).trim());
        }
        return defaultValue;
    }

    private static List<Message> buildBathMessage(final int batchSize, final String topic) {
        List<Message> batchMessage = new ArrayList<>(batchSize);
        for (int i = 0; i < batchSize; i++) {
            Message msg = new Message(topic, msgBody);
            batchMessage.add(msg);
        }
        return batchMessage;
    }

    private static void setKeys(boolean keyEnable, List<Message> msgs, String keys) {
        if (!keyEnable) {
            return;
        }

        for (Message msg : msgs) {
            msg.setKeys(keys);
        }
    }

    private static void setTags(int tagCount, List<Message> msgs, long startTagId) {
        if (tagCount <= 0) {
            return;
        }

        long tagId = startTagId % tagCount;
        for (Message msg : msgs) {
            msg.setTags(String.format("tag%d", tagId++));
        }
    }

    private static void setProperties(int propertySize, List<Message> msgs) {
        if (propertySize <= 0) {
            return;
        }

        for (Message msg : msgs) {
            if (msg.getProperties() != null) {
                msg.getProperties().clear();
            }

            int startValue = (new Random(System.currentTimeMillis())).nextInt(100);
            int size = 0;
            for (int i = 0; ; i++) {
                String prop1 = "prop" + i, prop1V = "hello" + startValue;
                msg.putUserProperty(prop1, prop1V);
                size += prop1.length() + prop1V.length();
                if (size > propertySize) {
                    break;
                }
                startValue++;
            }
        }
    }

    private static DefaultMQProducer initInstance(String namesrv, boolean traceEnable, boolean aclEnable, String ak,
                                                  String sk) {
        RPCHook rpcHook = aclEnable ? new AclClientRPCHook(new SessionCredentials(ak, sk)) : null;
        final DefaultMQProducer producer = new DefaultMQProducer("benchmark_batch_producer", rpcHook, traceEnable, null);
        producer.setInstanceName(Long.toString(System.currentTimeMillis()));

        producer.setNamesrvAddr(namesrv);
        producer.setCompressMsgBodyOverHowmuch(Integer.MAX_VALUE);
        return producer;
    }
}

class StatsBenchmarkBatchProducer {

    private final LongAdder sendRequestSuccessCount = new LongAdder();

    private final LongAdder sendRequestFailedCount = new LongAdder();

    private final LongAdder sendMessageSuccessTimeTotal = new LongAdder();

    private final AtomicLong sendMessageMaxRT = new AtomicLong(0L);

    private final LongAdder sendMessageSuccessCount = new LongAdder();

    private final LongAdder sendMessageFailedCount = new LongAdder();

    private final ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl(
            "BenchmarkTimerThread", Boolean.TRUE));

    private final LinkedList<Long[]> snapshotList = new LinkedList<>();

    public Long[] createSnapshot() {
        Long[] snap = new Long[] {
                System.currentTimeMillis(),
                this.sendRequestSuccessCount.longValue(),
                this.sendRequestFailedCount.longValue(),
                this.sendMessageSuccessCount.longValue(),
                this.sendMessageFailedCount.longValue(),
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

    public LongAdder getSendMessageSuccessTimeTotal() {
        return sendMessageSuccessTimeTotal;
    }

    public AtomicLong getSendMessageMaxRT() {
        return sendMessageMaxRT;
    }

    public LongAdder getSendMessageSuccessCount() {
        return sendMessageSuccessCount;
    }

    public LongAdder getSendMessageFailedCount() {
        return sendMessageFailedCount;
    }

    public void start() {

        executorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                snapshotList.addLast(createSnapshot());
                if (snapshotList.size() > 10) {
                    snapshotList.removeFirst();
                }
            }
        }, 1000, 1000, TimeUnit.MILLISECONDS);

        executorService.scheduleAtFixedRate(new Runnable() {
            private void printStats() {
                if (snapshotList.size() >= 10) {
                    Long[] begin = snapshotList.getFirst();
                    Long[] end = snapshotList.getLast();

                    final long sendTps = (long) (((end[1] - begin[1]) / (double) (end[0] - begin[0])) * 1000L);
                    final long sendMps = (long) (((end[3] - begin[3]) / (double) (end[0] - begin[0])) * 1000L);
                    final double averageRT = (end[5] - begin[5]) / (double) (end[1] - begin[1]);
                    final double averageMsgRT = (end[5] - begin[5]) / (double) (end[3] - begin[3]);

                    System.out.printf("Current Time: %s Send TPS: %d Send MPS: %d Max RT(ms): %d Average RT(ms): %7.3f Average Message RT(ms): %7.3f Send Failed: %d Send Message Failed: %d%n",
                            System.currentTimeMillis(), sendTps, sendMps, getSendMessageMaxRT().longValue(), averageRT, averageMsgRT, end[2], end[4]);
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
    }

    public void shutdown() {
        executorService.shutdown();
    }
}