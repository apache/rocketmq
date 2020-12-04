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

import java.io.UnsupportedEncodingException;
import java.util.LinkedList;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.srvutil.ServerUtil;

public class Producer {

    public static void main(String[] args) throws MQClientException, UnsupportedEncodingException {

        Options options = ServerUtil.buildCommandlineOptions(new Options());
        CommandLine commandLine = ServerUtil.parseCmdLine("benchmarkProducer", args, buildCommandlineOptions(options), new PosixParser());
        if (null == commandLine) {
            System.exit(-1);
        }

        final String topic = commandLine.hasOption('t') ? commandLine.getOptionValue('t').trim() : "BenchmarkTest";
        final int threadCount = commandLine.hasOption('w') ? Integer.parseInt(commandLine.getOptionValue('w')) : 64;
        final int messageSize = commandLine.hasOption('s') ? Integer.parseInt(commandLine.getOptionValue('s')) : 128;
        final boolean keyEnable = commandLine.hasOption('k') && Boolean.parseBoolean(commandLine.getOptionValue('k'));
        final int propertySize = commandLine.hasOption('p') ? Integer.parseInt(commandLine.getOptionValue('p')) : 0;
        final int tagCount = commandLine.hasOption('l') ? Integer.parseInt(commandLine.getOptionValue('l')) : 0;
        final boolean msgTraceEnable = commandLine.hasOption('m') && Boolean.parseBoolean(commandLine.getOptionValue('m'));
        final boolean aclEnable = commandLine.hasOption('a') && Boolean.parseBoolean(commandLine.getOptionValue('a'));

        System.out.printf("topic: %s threadCount: %d messageSize: %d keyEnable: %s propertySize: %d tagCount: %d traceEnable: %s aclEnable: %s%n",
            topic, threadCount, messageSize, keyEnable, propertySize, tagCount, msgTraceEnable, aclEnable);

        final InternalLogger log = ClientLogger.getLog();

        final ExecutorService sendThreadPool = Executors.newFixedThreadPool(threadCount);

        final StatsBenchmarkProducer statsBenchmark = new StatsBenchmarkProducer();

        final Timer timer = new Timer("BenchmarkTimerThread", true);

        final LinkedList<Long[]> snapshotList = new LinkedList<Long[]>();

        timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                snapshotList.addLast(statsBenchmark.createSnapshot());
                if (snapshotList.size() > 10) {
                    snapshotList.removeFirst();
                }
            }
        }, 1000, 1000);

        timer.scheduleAtFixedRate(new TimerTask() {
            private void printStats() {
                if (snapshotList.size() >= 10) {
                    Long[] begin = snapshotList.getFirst();
                    Long[] end = snapshotList.getLast();

                    final long sendTps = (long) (((end[3] - begin[3]) / (double) (end[0] - begin[0])) * 1000L);
                    final double averageRT = (end[5] - begin[5]) / (double) (end[3] - begin[3]);

                    System.out.printf("Current Time: %s Send TPS: %d Max RT(ms): %d Average RT(ms): %7.3f Send Failed: %d Response Failed: %d%n",
                        System.currentTimeMillis(), sendTps, statsBenchmark.getSendMessageMaxRT().get(), averageRT, end[2], end[4]);
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
        }, 10000, 10000);

        RPCHook rpcHook = aclEnable ? AclClient.getAclRPCHook() : null;
        final DefaultMQProducer producer = new DefaultMQProducer("benchmark_producer", rpcHook, msgTraceEnable, null);
        producer.setInstanceName(Long.toString(System.currentTimeMillis()));

        if (commandLine.hasOption('n')) {
            String ns = commandLine.getOptionValue('n');
            producer.setNamesrvAddr(ns);
        }

        producer.setCompressMsgBodyOverHowmuch(Integer.MAX_VALUE);

        producer.start();

        for (int i = 0; i < threadCount; i++) {
            sendThreadPool.execute(new Runnable() {
                @Override
                public void run() {
                    while (true) {
                        try {
                            final Message msg;
                            try {
                                msg = buildMessage(messageSize, topic);
                            } catch (UnsupportedEncodingException e) {
                                e.printStackTrace();
                                return;
                            }
                            final long beginTimestamp = System.currentTimeMillis();
                            if (keyEnable) {
                                msg.setKeys(String.valueOf(beginTimestamp / 1000));
                            }
                            if (tagCount > 0) {
                                long sendSucCount = statsBenchmark.getReceiveResponseSuccessCount().get();
                                msg.setTags(String.format("tag%d", sendSucCount % tagCount));
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
                            producer.send(msg);
                            statsBenchmark.getSendRequestSuccessCount().incrementAndGet();
                            statsBenchmark.getReceiveResponseSuccessCount().incrementAndGet();
                            final long currentRT = System.currentTimeMillis() - beginTimestamp;
                            statsBenchmark.getSendMessageSuccessTimeTotal().addAndGet(currentRT);
                            long prevMaxRT = statsBenchmark.getSendMessageMaxRT().get();
                            while (currentRT > prevMaxRT) {
                                boolean updated = statsBenchmark.getSendMessageMaxRT().compareAndSet(prevMaxRT, currentRT);
                                if (updated)
                                    break;

                                prevMaxRT = statsBenchmark.getSendMessageMaxRT().get();
                            }
                        } catch (RemotingException e) {
                            statsBenchmark.getSendRequestFailedCount().incrementAndGet();
                            log.error("[BENCHMARK_PRODUCER] Send Exception", e);

                            try {
                                Thread.sleep(3000);
                            } catch (InterruptedException ignored) {
                            }
                        } catch (InterruptedException e) {
                            statsBenchmark.getSendRequestFailedCount().incrementAndGet();
                            try {
                                Thread.sleep(3000);
                            } catch (InterruptedException e1) {
                            }
                        } catch (MQClientException e) {
                            statsBenchmark.getSendRequestFailedCount().incrementAndGet();
                            log.error("[BENCHMARK_PRODUCER] Send Exception", e);
                        } catch (MQBrokerException e) {
                            statsBenchmark.getReceiveResponseFailedCount().incrementAndGet();
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

        return options;
    }

    private static Message buildMessage(final int messageSize, final String topic) throws UnsupportedEncodingException {
        Message msg = new Message();
        msg.setTopic(topic);

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < messageSize; i += 10) {
            sb.append("hello baby");
        }

        msg.setBody(sb.toString().getBytes(RemotingHelper.DEFAULT_CHARSET));

        return msg;
    }
}

class StatsBenchmarkProducer {
    private final AtomicLong sendRequestSuccessCount = new AtomicLong(0L);

    private final AtomicLong sendRequestFailedCount = new AtomicLong(0L);

    private final AtomicLong receiveResponseSuccessCount = new AtomicLong(0L);

    private final AtomicLong receiveResponseFailedCount = new AtomicLong(0L);

    private final AtomicLong sendMessageSuccessTimeTotal = new AtomicLong(0L);

    private final AtomicLong sendMessageMaxRT = new AtomicLong(0L);

    public Long[] createSnapshot() {
        Long[] snap = new Long[] {
            System.currentTimeMillis(),
            this.sendRequestSuccessCount.get(),
            this.sendRequestFailedCount.get(),
            this.receiveResponseSuccessCount.get(),
            this.receiveResponseFailedCount.get(),
            this.sendMessageSuccessTimeTotal.get(),
        };

        return snap;
    }

    public AtomicLong getSendRequestSuccessCount() {
        return sendRequestSuccessCount;
    }

    public AtomicLong getSendRequestFailedCount() {
        return sendRequestFailedCount;
    }

    public AtomicLong getReceiveResponseSuccessCount() {
        return receiveResponseSuccessCount;
    }

    public AtomicLong getReceiveResponseFailedCount() {
        return receiveResponseFailedCount;
    }

    public AtomicLong getSendMessageSuccessTimeTotal() {
        return sendMessageSuccessTimeTotal;
    }

    public AtomicLong getSendMessageMaxRT() {
        return sendMessageMaxRT;
    }
}
