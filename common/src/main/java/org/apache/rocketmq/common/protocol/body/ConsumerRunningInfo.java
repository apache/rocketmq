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

package org.apache.rocketmq.common.protocol.body;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.TreeMap;
import java.util.TreeSet;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

public class ConsumerRunningInfo extends RemotingSerializable {
    public static final String PROP_NAMESERVER_ADDR = "PROP_NAMESERVER_ADDR";
    public static final String PROP_THREADPOOL_CORE_SIZE = "PROP_THREADPOOL_CORE_SIZE";
    public static final String PROP_CONSUME_ORDERLY = "PROP_CONSUMEORDERLY";
    public static final String PROP_CONSUME_TYPE = "PROP_CONSUME_TYPE";
    public static final String PROP_CLIENT_VERSION = "PROP_CLIENT_VERSION";
    public static final String PROP_CONSUMER_START_TIMESTAMP = "PROP_CONSUMER_START_TIMESTAMP";

    private Properties properties = new Properties();

    private TreeSet<SubscriptionData> subscriptionSet = new TreeSet<SubscriptionData>();

    private TreeMap<MessageQueue, ProcessQueueInfo> mqTable = new TreeMap<MessageQueue, ProcessQueueInfo>();

    private TreeMap<String/* Topic */, ConsumeStatus> statusTable = new TreeMap<String, ConsumeStatus>();

    private String jstack;

    public static boolean analyzeSubscription(final TreeMap<String/* clientId */, ConsumerRunningInfo> criTable) {
        ConsumerRunningInfo prev = criTable.firstEntry().getValue();

        boolean push = false;
        {
            String property = prev.getProperties().getProperty(ConsumerRunningInfo.PROP_CONSUME_TYPE);

            if (property == null) {
                property = ((ConsumeType) prev.getProperties().get(ConsumerRunningInfo.PROP_CONSUME_TYPE)).name();
            }
            push = ConsumeType.valueOf(property) == ConsumeType.CONSUME_PASSIVELY;
        }

        boolean startForAWhile = false;
        {

            String property = prev.getProperties().getProperty(ConsumerRunningInfo.PROP_CONSUMER_START_TIMESTAMP);
            if (property == null) {
                property = String.valueOf(prev.getProperties().get(ConsumerRunningInfo.PROP_CONSUMER_START_TIMESTAMP));
            }
            startForAWhile = (System.currentTimeMillis() - Long.parseLong(property)) > (1000 * 60 * 2);
        }

        if (push && startForAWhile) {

            {
                Iterator<Entry<String, ConsumerRunningInfo>> it = criTable.entrySet().iterator();
                while (it.hasNext()) {
                    Entry<String, ConsumerRunningInfo> next = it.next();
                    ConsumerRunningInfo current = next.getValue();
                    boolean equals = current.getSubscriptionSet().equals(prev.getSubscriptionSet());

                    if (!equals) {
                        // Different subscription in the same group of consumer
                        return false;
                    }

                    prev = next.getValue();
                }

                if (prev != null) {

                    if (prev.getSubscriptionSet().isEmpty()) {
                        // Subscription empty!
                        return false;
                    }
                }
            }
        }

        return true;
    }

    public static boolean analyzeRebalance(final TreeMap<String/* clientId */, ConsumerRunningInfo> criTable) {
        return true;
    }

    public static String analyzeProcessQueue(final String clientId, ConsumerRunningInfo info) {
        StringBuilder sb = new StringBuilder();
        boolean push = false;
        {
            String property = info.getProperties().getProperty(ConsumerRunningInfo.PROP_CONSUME_TYPE);

            if (property == null) {
                property = ((ConsumeType) info.getProperties().get(ConsumerRunningInfo.PROP_CONSUME_TYPE)).name();
            }
            push = ConsumeType.valueOf(property) == ConsumeType.CONSUME_PASSIVELY;
        }

        boolean orderMsg = false;
        {
            String property = info.getProperties().getProperty(ConsumerRunningInfo.PROP_CONSUME_ORDERLY);
            orderMsg = Boolean.parseBoolean(property);
        }

        if (push) {
            Iterator<Entry<MessageQueue, ProcessQueueInfo>> it = info.getMqTable().entrySet().iterator();
            while (it.hasNext()) {
                Entry<MessageQueue, ProcessQueueInfo> next = it.next();
                MessageQueue mq = next.getKey();
                ProcessQueueInfo pq = next.getValue();

                if (orderMsg) {

                    if (!pq.isLocked()) {
                        sb.append(String.format("%s %s can't lock for a while, %dms%n",
                            clientId,
                            mq,
                            System.currentTimeMillis() - pq.getLastLockTimestamp()));
                    } else {
                        if (pq.isDroped() && (pq.getTryUnlockTimes() > 0)) {
                            sb.append(String.format("%s %s unlock %d times, still failed%n",
                                clientId,
                                mq,
                                pq.getTryUnlockTimes()));
                        }
                    }

                } else {
                    long diff = System.currentTimeMillis() - pq.getLastConsumeTimestamp();

                    if (diff > (1000 * 60) && pq.getCachedMsgCount() > 0) {
                        sb.append(String.format("%s %s can't consume for a while, maybe blocked, %dms%n",
                            clientId,
                            mq,
                            diff));
                    }
                }
            }
        }

        return sb.toString();
    }

    public Properties getProperties() {
        return properties;
    }

    public void setProperties(Properties properties) {
        this.properties = properties;
    }

    public TreeSet<SubscriptionData> getSubscriptionSet() {
        return subscriptionSet;
    }

    public void setSubscriptionSet(TreeSet<SubscriptionData> subscriptionSet) {
        this.subscriptionSet = subscriptionSet;
    }

    public TreeMap<MessageQueue, ProcessQueueInfo> getMqTable() {
        return mqTable;
    }

    public void setMqTable(TreeMap<MessageQueue, ProcessQueueInfo> mqTable) {
        this.mqTable = mqTable;
    }

    public TreeMap<String, ConsumeStatus> getStatusTable() {
        return statusTable;
    }

    public void setStatusTable(TreeMap<String, ConsumeStatus> statusTable) {
        this.statusTable = statusTable;
    }

    public String formatString() {
        StringBuilder sb = new StringBuilder();

        {
            sb.append("#Consumer Properties#\n");
            Iterator<Entry<Object, Object>> it = this.properties.entrySet().iterator();
            while (it.hasNext()) {
                Entry<Object, Object> next = it.next();
                String item = String.format("%-40s: %s%n", next.getKey().toString(), next.getValue().toString());
                sb.append(item);
            }
        }

        {
            sb.append("\n\n#Consumer Subscription#\n");

            Iterator<SubscriptionData> it = this.subscriptionSet.iterator();
            int i = 0;
            while (it.hasNext()) {
                SubscriptionData next = it.next();
                String item = String.format("%03d Topic: %-40s ClassFilter: %-8s SubExpression: %s%n",
                    ++i,
                    next.getTopic(),
                    next.isClassFilterMode(),
                    next.getSubString());

                sb.append(item);
            }
        }

        {
            sb.append("\n\n#Consumer Offset#\n");
            sb.append(String.format("%-64s  %-32s  %-4s  %-20s%n",
                "#Topic",
                "#Broker Name",
                "#QID",
                "#Consumer Offset"
            ));

            Iterator<Entry<MessageQueue, ProcessQueueInfo>> it = this.mqTable.entrySet().iterator();
            while (it.hasNext()) {
                Entry<MessageQueue, ProcessQueueInfo> next = it.next();
                String item = String.format("%-32s  %-32s  %-4d  %-20d%n",
                    next.getKey().getTopic(),
                    next.getKey().getBrokerName(),
                    next.getKey().getQueueId(),
                    next.getValue().getCommitOffset());

                sb.append(item);
            }
        }

        {
            sb.append("\n\n#Consumer MQ Detail#\n");
            sb.append(String.format("%-64s  %-32s  %-4s  %-20s%n",
                "#Topic",
                "#Broker Name",
                "#QID",
                "#ProcessQueueInfo"
            ));

            Iterator<Entry<MessageQueue, ProcessQueueInfo>> it = this.mqTable.entrySet().iterator();
            while (it.hasNext()) {
                Entry<MessageQueue, ProcessQueueInfo> next = it.next();
                String item = String.format("%-64s  %-32s  %-4d  %s%n",
                    next.getKey().getTopic(),
                    next.getKey().getBrokerName(),
                    next.getKey().getQueueId(),
                    next.getValue().toString());

                sb.append(item);
            }
        }

        {
            sb.append("\n\n#Consumer RT&TPS#\n");
            sb.append(String.format("%-64s  %14s %14s %14s %14s %18s %25s%n",
                "#Topic",
                "#Pull RT",
                "#Pull TPS",
                "#Consume RT",
                "#ConsumeOK TPS",
                "#ConsumeFailed TPS",
                "#ConsumeFailedMsgsInHour"
            ));

            Iterator<Entry<String, ConsumeStatus>> it = this.statusTable.entrySet().iterator();
            while (it.hasNext()) {
                Entry<String, ConsumeStatus> next = it.next();
                String item = String.format("%-32s  %14.2f %14.2f %14.2f %14.2f %18.2f %25d%n",
                    next.getKey(),
                    next.getValue().getPullRT(),
                    next.getValue().getPullTPS(),
                    next.getValue().getConsumeRT(),
                    next.getValue().getConsumeOKTPS(),
                    next.getValue().getConsumeFailedTPS(),
                    next.getValue().getConsumeFailedMsgs()
                );

                sb.append(item);
            }
        }

        if (this.jstack != null) {
            sb.append("\n\n#Consumer jstack#\n");
            sb.append(this.jstack);
        }

        return sb.toString();
    }

    public String getJstack() {
        return jstack;
    }

    public void setJstack(String jstack) {
        this.jstack = jstack;
    }

}
