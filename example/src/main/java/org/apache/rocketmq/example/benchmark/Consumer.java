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

import java.util.concurrent.atomic.LongAdder;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.MessageSelector;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.consumer.rebalance.AllocateMessageQueueAveragely;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.filter.ExpressionType;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.SerializeType;
import org.apache.rocketmq.srvutil.ServerUtil;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.TimerTask;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class Consumer {

    public static void main(String[] args) throws MQClientException, IOException {
        System.setProperty(RemotingCommand.SERIALIZE_TYPE_PROPERTY, SerializeType.ROCKETMQ.name());
        Options options = ServerUtil.buildCommandlineOptions(new Options());
        CommandLine commandLine = ServerUtil.parseCmdLine("benchmarkConsumer", args, buildCommandlineOptions(options), new PosixParser());
        if (null == commandLine) {
            System.exit(-1);
        }

        final String topic = commandLine.hasOption('t') ? commandLine.getOptionValue('t').trim() : "BenchmarkTest";
        final int threadCount = commandLine.hasOption('w') ? Integer.parseInt(commandLine.getOptionValue('w')) : 20;
        final String groupPrefix = commandLine.hasOption('g') ? commandLine.getOptionValue('g').trim() : "benchmark_consumer";
        final String isSuffixEnable = commandLine.hasOption('p') ? commandLine.getOptionValue('p').trim() : "false";
        final String filterType = commandLine.hasOption('f') ? commandLine.getOptionValue('f').trim() : null;
        final String expression = commandLine.hasOption('e') ? commandLine.getOptionValue('e').trim() : null;
        final double failRate = commandLine.hasOption('r') ? Double.parseDouble(commandLine.getOptionValue('r').trim()) : 0.0;
        final boolean msgTraceEnable = commandLine.hasOption('m') && Boolean.parseBoolean(commandLine.getOptionValue('m'));
        final boolean aclEnable = commandLine.hasOption('a') && Boolean.parseBoolean(commandLine.getOptionValue('a'));

        String group = groupPrefix;
        if (Boolean.parseBoolean(isSuffixEnable)) {
            group = groupPrefix + "_" + (System.currentTimeMillis() % 100);
        }

        System.out.printf("topic: %s, threadCount %d, group: %s, suffix: %s, filterType: %s, expression: %s, msgTraceEnable: %s, aclEnable: %s%n",
            topic, threadCount, group, isSuffixEnable, filterType, expression, msgTraceEnable, aclEnable);

        final StatsBenchmarkConsumer statsBenchmarkConsumer = new StatsBenchmarkConsumer();

        ScheduledExecutorService executorService = new ScheduledThreadPoolExecutor(1,
                new BasicThreadFactory.Builder().namingPattern("BenchmarkTimerThread-%d").daemon(true).build());

        final LinkedList<Long[]> snapshotList = new LinkedList<Long[]>();

        executorService.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                snapshotList.addLast(statsBenchmarkConsumer.createSnapshot());
                if (snapshotList.size() > 10) {
                    snapshotList.removeFirst();
                }
            }
        }, 1000, 1000, TimeUnit.MILLISECONDS);

        executorService.scheduleAtFixedRate(new TimerTask() {
            private void printStats() {
                if (snapshotList.size() >= 10) {
                    Long[] begin = snapshotList.getFirst();
                    Long[] end = snapshotList.getLast();

                    final long consumeTps =
                        (long) (((end[1] - begin[1]) / (double) (end[0] - begin[0])) * 1000L);
                    final double averageB2CRT = (end[2] - begin[2]) / (double) (end[1] - begin[1]);
                    final double averageS2CRT = (end[3] - begin[3]) / (double) (end[1] - begin[1]);
                    final long failCount = end[4] - begin[4];
                    final long b2cMax = statsBenchmarkConsumer.getBorn2ConsumerMaxRT().get();
                    final long s2cMax = statsBenchmarkConsumer.getStore2ConsumerMaxRT().get();

                    statsBenchmarkConsumer.getBorn2ConsumerMaxRT().set(0);
                    statsBenchmarkConsumer.getStore2ConsumerMaxRT().set(0);

                    System.out.printf("Current Time: %s TPS: %d FAIL: %d AVG(B2C) RT(ms): %7.3f AVG(S2C) RT(ms): %7.3f MAX(B2C) RT(ms): %d MAX(S2C) RT(ms): %d%n",
                            System.currentTimeMillis(), consumeTps, failCount, averageB2CRT, averageS2CRT, b2cMax, s2cMax
                    );
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
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(group, rpcHook, new AllocateMessageQueueAveragely(), msgTraceEnable, null);
        if (commandLine.hasOption('n')) {
            String ns = commandLine.getOptionValue('n');
            consumer.setNamesrvAddr(ns);
        }
        consumer.setConsumeThreadMin(threadCount);
        consumer.setConsumeThreadMax(threadCount);
        consumer.setInstanceName(Long.toString(System.currentTimeMillis()));

        if (filterType == null || expression == null) {
            consumer.subscribe(topic, "*");
        } else {
            if (ExpressionType.TAG.equals(filterType)) {
                String expr = MixAll.file2String(expression);
                System.out.printf("Expression: %s%n", expr);
                consumer.subscribe(topic, MessageSelector.byTag(expr));
            } else if (ExpressionType.SQL92.equals(filterType)) {
                String expr = MixAll.file2String(expression);
                System.out.printf("Expression: %s%n", expr);
                consumer.subscribe(topic, MessageSelector.bySql(expr));
            } else {
                throw new IllegalArgumentException("Not support filter type! " + filterType);
            }
        }

        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
                ConsumeConcurrentlyContext context) {
                MessageExt msg = msgs.get(0);
                long now = System.currentTimeMillis();

                statsBenchmarkConsumer.getReceiveMessageTotalCount().increment();

                long born2ConsumerRT = now - msg.getBornTimestamp();
                statsBenchmarkConsumer.getBorn2ConsumerTotalRT().add(born2ConsumerRT);

                long store2ConsumerRT = now - msg.getStoreTimestamp();
                statsBenchmarkConsumer.getStore2ConsumerTotalRT().add(store2ConsumerRT);

                compareAndSetMax(statsBenchmarkConsumer.getBorn2ConsumerMaxRT(), born2ConsumerRT);

                compareAndSetMax(statsBenchmarkConsumer.getStore2ConsumerMaxRT(), store2ConsumerRT);

                if (ThreadLocalRandom.current().nextDouble() < failRate) {
                    statsBenchmarkConsumer.getFailCount().increment();
                    return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                } else {
                    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                }
            }
        });

        consumer.start();

        System.out.printf("Consumer Started.%n");
    }

    public static Options buildCommandlineOptions(final Options options) {
        Option opt = new Option("t", "topic", true, "Topic name, Default: BenchmarkTest");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("w", "threadCount", true, "Thread count, Default: 20");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("g", "group", true, "Consumer group name, Default: benchmark_consumer");
        opt.setRequired(false);
        options.addOption(opt);
        opt = new Option("p", "group prefix enable", true, "Is group prefix enable, Default: false");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("f", "filterType", true, "TAG, SQL92");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("e", "expression", true, "filter expression content file path.ie: ./test/expr");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("r", "fail rate", true, "consumer fail rate, default 0");
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

        return options;
    }

    public static void compareAndSetMax(final AtomicLong target, final long value) {
        long prev = target.get();
        while (value > prev) {
            boolean updated = target.compareAndSet(prev, value);
            if (updated)
                break;

            prev = target.get();
        }
    }
}

class StatsBenchmarkConsumer {
    private final LongAdder receiveMessageTotalCount = new LongAdder();

    private final LongAdder born2ConsumerTotalRT = new LongAdder();

    private final LongAdder store2ConsumerTotalRT = new LongAdder();

    private final AtomicLong born2ConsumerMaxRT = new AtomicLong(0L);

    private final AtomicLong store2ConsumerMaxRT = new AtomicLong(0L);

    private final LongAdder failCount = new LongAdder();

    public Long[] createSnapshot() {
        Long[] snap = new Long[] {
            System.currentTimeMillis(),
            this.receiveMessageTotalCount.longValue(),
            this.born2ConsumerTotalRT.longValue(),
            this.store2ConsumerTotalRT.longValue(),
            this.failCount.longValue()
        };

        return snap;
    }

    public LongAdder getReceiveMessageTotalCount() {
        return receiveMessageTotalCount;
    }

    public LongAdder getBorn2ConsumerTotalRT() {
        return born2ConsumerTotalRT;
    }

    public LongAdder getStore2ConsumerTotalRT() {
        return store2ConsumerTotalRT;
    }

    public AtomicLong getBorn2ConsumerMaxRT() {
        return born2ConsumerMaxRT;
    }

    public AtomicLong getStore2ConsumerMaxRT() {
        return store2ConsumerMaxRT;
    }

    public LongAdder getFailCount() {
        return failCount;
    }
}
