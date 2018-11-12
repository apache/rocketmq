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
package org.apache.rocketmq.tools.command.cluster;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.body.ClusterInfo;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.SubCommand;
import org.apache.rocketmq.tools.command.SubCommandException;

import static org.apache.rocketmq.common.MixAll.TOOLS_CONSUMER_GROUP;

public class ClusterCheckSubCommand implements SubCommand {
    private DefaultMQProducer producer;
    private DefaultMQAdminExt adminExt;
    private DefaultMQPushConsumer consumer;

    @Override
    public String commandName() {
        return "clusterCheck";
    }

    @Override
    public String commandDesc() {
        return "check cluster read/write";
    }

    @Override
    public Options buildCommandlineOptions(Options options) {
        Option opt = new Option("c", "cluster", true, "witch cluster to check, required");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("t", "count", true, "message count to each broker, default 10, less than 1000");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("s", "seconds", true, "wait consume complete seconds, default 10, less than 100");
        opt.setRequired(false);
        options.addOption(opt);

        return options;
    }

    @Override
    public void execute(CommandLine commandLine, Options options,
                        RPCHook rpcHook) throws SubCommandException {
        long current = System.currentTimeMillis();
        if (producer == null) {
            producer = new DefaultMQProducer("check");
        }
        producer.setInstanceName(String.valueOf(current));
        if (adminExt == null) {
            adminExt = new DefaultMQAdminExt(rpcHook);
        }
        adminExt.setInstanceName(String.valueOf(current + 1));
        if (consumer == null) {
            consumer = new DefaultMQPushConsumer(TOOLS_CONSUMER_GROUP);
        }
        consumer.setInstanceName(String.valueOf(current + 2));
        String cName = commandLine.hasOption('c') ? commandLine.getOptionValue('c').trim() : null;
        int count = commandLine.hasOption('t') ? Integer.parseInt(commandLine.getOptionValue('t')) : 10;
        if (count > 1000) {
            count = 1000;
        }
        int wait = commandLine.hasOption('s') ? Integer.parseInt(commandLine.getOptionValue('s')) : 10;
        if (wait > 100) {
            wait = 100;
        }
        final Map<String/* clusterName */, ConcurrentHashMap<String/* brokerName */, Counter>> clusterMap = new ConcurrentHashMap<>();
        consumer.setConsumeThreadMin(20);
        consumer.setConsumeThreadMax(20);
        consumer.setMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
                                                            ConsumeConcurrentlyContext context) {
                for (MessageExt msg : msgs) {
                    String s = new String(msg.getBody());
                    String[] cb = s.split("##");
                    if (cb.length == 2) {
                        clusterMap.get(cb[0]).get(cb[1]).incrConsumeSuccess();
                    }
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        try {
            adminExt.start();
            producer.start();
            ClusterInfo clusterInfo = adminExt.examineBrokerClusterInfo();
            HashMap<String, Set<String>> clusterTable = clusterInfo.getClusterAddrTable();
            for (Map.Entry<String, Set<String>> entry : clusterTable.entrySet()) {
                String clusterName = entry.getKey();
                if (cName == null || clusterName.equals(cName)) {
                    if (!clusterMap.containsKey(clusterName)) {
                        clusterMap.put(clusterName, new ConcurrentHashMap<String/* brokerName */, Counter>());
                    }
                    final ConcurrentHashMap<String, Counter> brokerMap = clusterMap.get(clusterName);
                    for (final String brokerName : entry.getValue()) {
                        if (!brokerMap.containsKey(brokerName)) {
                            brokerMap.put(brokerName, new Counter());
                            consumer.subscribe(brokerName, "*");
                        }
                        final Message message = new Message(brokerName, (clusterName + "##" + brokerName).getBytes());

                        final int finalCount = count;
                        Thread thread = new Thread(new Runnable() {
                            @Override
                            public void run() {
                                for (int i = 0; i < finalCount; i++) {
                                    try {
                                        producer.send(message);
                                        brokerMap.get(brokerName).incrProduceSuccess();
                                    } catch (Exception e) {
                                        brokerMap.get(brokerName).incrProduceFail();
                                    }
                                }
                            }
                        });
                        thread.start();
                    }
                }
            }
            consumer.start();
        } catch (Exception e) {
            throw new SubCommandException(this.getClass().getSimpleName() + " command failed", e);
        } finally {

        }
        System.out.print("waiting for producer and consumer");
        for (int i = 0; i < wait; i++) {
            System.out.print(".");
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        System.out.printf("%n%-24s  %-24s  %-16s  %-16s  %-16s%n",
                "#clusterName",
                "#brokerName",
                "#produceSuccess",
                "#produceFail",
                "#consumeSuccess");
        for (Map.Entry<String/* clusterName */, ConcurrentHashMap<String/* brokerName */, Counter>> entry : clusterMap.entrySet()) {
            String clusterName = entry.getKey();
            for (Map.Entry<String/* brokerName */, Counter> entry2 : entry.getValue().entrySet()) {
                String brokerName = entry2.getKey();
                Counter counter = entry2.getValue();
                System.out.printf("%-24s  %-24s  %-16s  %-16s  %-16s%n",
                        clusterName,
                        brokerName,
                        counter.produceSuccess,
                        counter.produceFail,
                        counter.consumeSuccess);
            }
        }
        if (consumer != null) {
            consumer.shutdown();
        }
        if (producer != null) {
            producer.shutdown();
        }
        if (adminExt != null) {
            adminExt.shutdown();
        }
    }
}

class Counter {
    int produceSuccess = 0;
    int produceFail = 0;
    int consumeSuccess = 0;

    void incrProduceSuccess() {
        produceSuccess++;
    }

    void incrProduceFail() {
        produceFail++;
    }

    void incrConsumeSuccess() {
        consumeSuccess++;
    }
}
