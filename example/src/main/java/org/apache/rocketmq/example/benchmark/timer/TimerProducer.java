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
package org.apache.rocketmq.example.benchmark.timer;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.srvutil.ServerUtil;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.TimerTask;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class TimerProducer {
    private static final InternalLogger LOGGER = ClientLogger.getLog();

    private final String topic;
    private final int threadCount;
    private final int messageSize;

    private final int precisionMs;
    private final int slotsTotal;
    private final int msgsTotalPerSlotThread;
    private final int slotDis;

    private final ScheduledExecutorService scheduledExecutor = new ScheduledThreadPoolExecutor(1, new ThreadFactoryImpl("ProducerScheduleThread_"));
    private final ExecutorService sendThreadPool;

    private final StatsBenchmarkProducer statsBenchmark = new StatsBenchmarkProducer();
    private final LinkedList<Long[]> snapshotList = new LinkedList<Long[]>();

    private final DefaultMQProducer producer;

    public TimerProducer(String[] args) {
        Options options = ServerUtil.buildCommandlineOptions(new Options());
        final CommandLine commandLine = ServerUtil.parseCmdLine("benchmarkTimerProducer", args, buildCommandlineOptions(options), new DefaultParser());
        if (null == commandLine) {
            System.exit(-1);
        }

        final String namesrvAddr = commandLine.hasOption('n') ? commandLine.getOptionValue('t').trim() : "localhost:9876";
        topic = commandLine.hasOption('t') ? commandLine.getOptionValue('t').trim() : "BenchmarkTest";
        threadCount = commandLine.hasOption("tc") ? Integer.parseInt(commandLine.getOptionValue("tc")) : 16;
        messageSize = commandLine.hasOption("ms") ? Integer.parseInt(commandLine.getOptionValue("ms")) : 1024;
        precisionMs = commandLine.hasOption('p') ? Integer.parseInt(commandLine.getOptionValue("p")) : 1000;
        slotsTotal = commandLine.hasOption("st") ? Integer.parseInt(commandLine.getOptionValue("st")) : 100;
        msgsTotalPerSlotThread = commandLine.hasOption("mt") ? Integer.parseInt(commandLine.getOptionValue("mt")) : 5000;
        slotDis = commandLine.hasOption("sd") ? Integer.parseInt(commandLine.getOptionValue("sd")) : 1000;
        System.out.printf("namesrvAddr: %s, topic: %s, threadCount: %d, messageSize: %d, precisionMs: %d, slotsTotal: %d, msgsTotalPerSlotThread: %d, slotDis: %d%n",
                namesrvAddr, topic, threadCount, messageSize, precisionMs, slotsTotal, msgsTotalPerSlotThread, slotDis);

        sendThreadPool = new ThreadPoolExecutor(
                threadCount,
                threadCount,
                0L,
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>(),
                new ThreadFactoryImpl("ProducerSendMessageThread_"));

        producer = new DefaultMQProducer("benchmark_producer");
        producer.setInstanceName(Long.toString(System.currentTimeMillis()));
        producer.setNamesrvAddr(namesrvAddr);
        producer.setCompressMsgBodyOverHowmuch(Integer.MAX_VALUE);
    }

    public void startScheduleTask() {
        scheduledExecutor.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                snapshotList.addLast(statsBenchmark.createSnapshot());
                if (snapshotList.size() > 10) {
                    snapshotList.removeFirst();
                }
            }
        }, 1000, 1000, TimeUnit.MILLISECONDS);

        scheduledExecutor.scheduleAtFixedRate(new TimerTask() {
            private void printStats() {
                if (snapshotList.size() >= 10) {
                    Long[] begin = snapshotList.getFirst();
                    Long[] end = snapshotList.getLast();

                    final long sendTps = (long) (((end[3] - begin[3]) / (double) (end[0] - begin[0])) * 1000L);
                    final double averageRT = (end[5] - begin[5]) / (double) (end[3] - begin[3]);

                    System.out.printf("Send TPS: %d, Max RT: %d, Average RT: %7.3f, Send Failed: %d, Response Failed: %d%n",
                            sendTps, statsBenchmark.getSendMessageMaxRT().get(), averageRT, end[2], end[4]);
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

    public void start() throws MQClientException {
        producer.start();
        System.out.printf("Start sending messages%n");
        List<Long> delayList = new ArrayList<Long>();
        final long startDelayTime = System.currentTimeMillis() / precisionMs * precisionMs + 2 * 60 * 1000 + 10;
        for (int slotCnt = 0; slotCnt < slotsTotal; slotCnt++) {
            for (int msgCnt = 0; msgCnt < msgsTotalPerSlotThread; msgCnt++) {
                long delayTime = startDelayTime + slotCnt * slotDis;
                delayList.add(delayTime);
            }
        }
        Collections.shuffle(delayList);
        // DelayTime is from 2 minutes later.

        for (int i = 0; i < threadCount; i++) {
            sendThreadPool.execute(new Runnable() {
                @Override
                public void run() {
                    for (int slotCnt = 0; slotCnt < slotsTotal; slotCnt++) {

                        for (int msgCnt = 0; msgCnt < msgsTotalPerSlotThread; msgCnt++) {
                            final long beginTimestamp = System.currentTimeMillis();

                            long delayTime = delayList.get(slotCnt * msgsTotalPerSlotThread + msgCnt);

                            final Message msg;
                            try {
                                msg = buildMessage(messageSize, topic);
                            } catch (UnsupportedEncodingException e) {
                                e.printStackTrace();
                                return;
                            }
                            msg.putUserProperty("MY_RECORD_TIMER_DELIVER_MS", String.valueOf(delayTime));
                            msg.getProperties().put(MessageConst.PROPERTY_TIMER_DELIVER_MS, String.valueOf(delayTime));

                            try {
                                producer.send(msg);

                                statsBenchmark.getSendRequestSuccessCount().incrementAndGet();
                                statsBenchmark.getReceiveResponseSuccessCount().incrementAndGet();

                                final long currentRT = System.currentTimeMillis() - beginTimestamp;
                                statsBenchmark.getSendMessageSuccessTimeTotal().addAndGet(currentRT);

                                long prevMaxRT = statsBenchmark.getSendMessageMaxRT().get();
                                while (currentRT > prevMaxRT) {
                                    if (statsBenchmark.getSendMessageMaxRT().compareAndSet(prevMaxRT, currentRT)) {
                                        break;
                                    }
                                    prevMaxRT = statsBenchmark.getSendMessageMaxRT().get();
                                }
                            } catch (RemotingException e) {
                                statsBenchmark.getSendRequestFailedCount().incrementAndGet();
                                LOGGER.error("[BENCHMARK_PRODUCER] Send Exception", e);
                                sleep(3000);
                            } catch (InterruptedException e) {
                                statsBenchmark.getSendRequestFailedCount().incrementAndGet();
                                sleep(3000);
                            } catch (MQClientException e) {
                                statsBenchmark.getSendRequestFailedCount().incrementAndGet();
                                LOGGER.error("[BENCHMARK_PRODUCER] Send Exception", e);
                            } catch (MQBrokerException e) {
                                statsBenchmark.getReceiveResponseFailedCount().incrementAndGet();
                                LOGGER.error("[BENCHMARK_PRODUCER] Send Exception", e);
                                sleep(3000);
                            }
                        }

                    }
                }
            });
        }
    }

    private Options buildCommandlineOptions(Options options) {
        Option opt = new Option("n", "namesrvAddr", true, "Nameserver address, default: localhost:9876");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("t", "topic", true, "Send messages to which topic, default: BenchmarkTest");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("tc", "threadCount", true, "Thread count, default: 64");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("ms", "messageSize", true, "Message Size, default: 128");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("p", "precisionMs", true, "Precision (ms) for TimerMessage, default: 1000");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("st", "slotsTotal", true, "Send messages to how many slots, default: 100");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("mt", "msgsTotalPerSlotThread", true, "Messages total for each slot and each thread, default: 100");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("sd", "slotDis", true, "Time distance between two slots, default: 1000");
        opt.setRequired(false);
        options.addOption(opt);

        return options;
    }

    private Message buildMessage(final int messageSize, final String topic) throws UnsupportedEncodingException {
        Message msg = new Message();
        msg.setTopic(topic);

        String body = StringUtils.repeat('a', messageSize);
        msg.setBody(body.getBytes(RemotingHelper.DEFAULT_CHARSET));

        return msg;
    }

    private void sleep(long timeMs) {
        try {
            Thread.sleep(timeMs);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws MQClientException {
        TimerProducer timerProducer = new TimerProducer(args);
        timerProducer.startScheduleTask();
        timerProducer.start();
    }


    public static class StatsBenchmarkProducer {
        private final AtomicLong sendRequestSuccessCount = new AtomicLong(0L);

        private final AtomicLong sendRequestFailedCount = new AtomicLong(0L);

        private final AtomicLong receiveResponseSuccessCount = new AtomicLong(0L);

        private final AtomicLong receiveResponseFailedCount = new AtomicLong(0L);

        private final AtomicLong sendMessageSuccessTimeTotal = new AtomicLong(0L);

        private final AtomicLong sendMessageMaxRT = new AtomicLong(0L);

        public Long[] createSnapshot() {
            return new Long[]{
                    System.currentTimeMillis(),
                    this.sendRequestSuccessCount.get(),
                    this.sendRequestFailedCount.get(),
                    this.receiveResponseSuccessCount.get(),
                    this.receiveResponseFailedCount.get(),
                    this.sendMessageSuccessTimeTotal.get(),
            };
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

}
