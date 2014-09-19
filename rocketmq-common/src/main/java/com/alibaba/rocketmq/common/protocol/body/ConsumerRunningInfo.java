package com.alibaba.rocketmq.common.protocol.body;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.TreeMap;
import java.util.TreeSet;

import com.alibaba.rocketmq.common.message.MessageQueue;
import com.alibaba.rocketmq.common.protocol.heartbeat.ConsumeType;
import com.alibaba.rocketmq.common.protocol.heartbeat.SubscriptionData;
import com.alibaba.rocketmq.remoting.protocol.RemotingSerializable;


/**
 * Consumer内部数据结构
 */
public class ConsumerRunningInfo extends RemotingSerializable {
    public static final String PROP_NAMESERVER_ADDR = "PROP_NAMESERVER_ADDR";
    public static final String PROP_THREADPOOL_CORE_SIZE = "PROP_THREADPOOL_CORE_SIZE";
    public static final String PROP_CONSUME_ORDERLY = "PROP_CONSUMEORDERLY";
    public static final String PROP_CONSUME_TYPE = "PROP_CONSUME_TYPE";
    public static final String PROP_CLIENT_VERSION = "PROP_CLIENT_VERSION";
    public static final String PROP_CONSUMER_START_TIMESTAMP = "PROP_CONSUMER_START_TIMESTAMP";

    // 各种配置及运行数据
    private Properties properties = new Properties();
    // 订阅关系
    private TreeSet<SubscriptionData> subscriptionSet = new TreeSet<SubscriptionData>();
    // 消费进度、Rebalance、内部消费队列的信息
    private TreeMap<MessageQueue, ProcessQueueInfo> mqTable = new TreeMap<MessageQueue, ProcessQueueInfo>();
    // RT、TPS统计
    private TreeMap<String/* Topic */, ConsumeStatus> statusTable = new TreeMap<String, ConsumeStatus>();
    // jstack的结果
    private String jstack;


    public Properties getProperties() {
        return properties;
    }


    public void setProperties(Properties properties) {
        this.properties = properties;
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


    public TreeSet<SubscriptionData> getSubscriptionSet() {
        return subscriptionSet;
    }


    public void setSubscriptionSet(TreeSet<SubscriptionData> subscriptionSet) {
        this.subscriptionSet = subscriptionSet;
    }


    public String formatString() {
        StringBuilder sb = new StringBuilder();

        // 1
        {
            sb.append("#Consumer Properties#\n");
            Iterator<Entry<Object, Object>> it = this.properties.entrySet().iterator();
            while (it.hasNext()) {
                Entry<Object, Object> next = it.next();
                String item =
                        String.format("%-40s: %s\n", next.getKey().toString(), next.getValue().toString());
                sb.append(item);
            }
        }

        // 2
        {
            sb.append("\n\n#Consumer Subscription#\n");

            Iterator<SubscriptionData> it = this.subscriptionSet.iterator();
            int i = 0;
            while (it.hasNext()) {
                SubscriptionData next = it.next();
                String item = String.format("%03d Topic: %-40s ClassFilter: %-8s SubExpression: %s\n", //
                    ++i,//
                    next.getTopic(),//
                    next.isClassFilterMode(),//
                    next.getSubString());

                sb.append(item);
            }
        }

        // 3
        {
            sb.append("\n\n#Consumer Offset#\n");
            sb.append(String.format("%-32s  %-32s  %-4s  %-20s\n",//
                "#Topic",//
                "#Broker Name",//
                "#QID",//
                "#Consumer Offset"//
            ));

            Iterator<Entry<MessageQueue, ProcessQueueInfo>> it = this.mqTable.entrySet().iterator();
            while (it.hasNext()) {
                Entry<MessageQueue, ProcessQueueInfo> next = it.next();
                String item = String.format("%-32s  %-32s  %-4d  %-20d\n",//
                    next.getKey().getTopic(),//
                    next.getKey().getBrokerName(),//
                    next.getKey().getQueueId(),//
                    next.getValue().getCommitOffset());

                sb.append(item);
            }
        }

        // 4
        {
            sb.append("\n\n#Consumer MQ Detail#\n");
            sb.append(String.format("%-32s  %-32s  %-4s  %-20s\n",//
                "#Topic",//
                "#Broker Name",//
                "#QID",//
                "#ProcessQueueInfo"//
            ));

            Iterator<Entry<MessageQueue, ProcessQueueInfo>> it = this.mqTable.entrySet().iterator();
            while (it.hasNext()) {
                Entry<MessageQueue, ProcessQueueInfo> next = it.next();
                String item = String.format("%-32s  %-32s  %-4d  %s\n",//
                    next.getKey().getTopic(),//
                    next.getKey().getBrokerName(),//
                    next.getKey().getQueueId(),//
                    next.getValue().toString());

                sb.append(item);
            }
        }

        // 5
        {
            sb.append("\n\n#Consumer RT&TPS#\n");
            sb.append(String.format("%-32s  %14s %14s %14s %14s %18s %25s\n",//
                "#Topic",//
                "#Pull RT",//
                "#Pull TPS",//
                "#Consume RT",//
                "#ConsumeOK TPS",//
                "#ConsumeFailed TPS",//
                "#ConsumeFailedMsgsInHour"//
            ));

            Iterator<Entry<String, ConsumeStatus>> it = this.statusTable.entrySet().iterator();
            while (it.hasNext()) {
                Entry<String, ConsumeStatus> next = it.next();
                String item = String.format("%-32s  %14.2f %14.2f %14.2f %14.2f %18.2f %25d\n",//
                    next.getKey(),//
                    next.getValue().getPullRT(),//
                    next.getValue().getPullTPS(),//
                    next.getValue().getConsumeRT(),//
                    next.getValue().getConsumeOKTPS(),//
                    next.getValue().getConsumeFailedTPS(),//
                    next.getValue().getConsumeFailedMsgs()//
                    );

                sb.append(item);
            }
        }

        // 6
        if (this.jstack != null) {
            sb.append("\n\n#Consumer jstack#\n");
            sb.append(this.jstack);
        }

        return sb.toString();
    }


    /**
     * 分析订阅关系是否相同
     */
    public static boolean analyzeSubscription(
            final TreeMap<String/* clientId */, ConsumerRunningInfo> criTable) {
        ConsumerRunningInfo prev = criTable.firstEntry().getValue();

        boolean push = false;
        {
            String property = prev.getProperties().getProperty(ConsumerRunningInfo.PROP_CONSUME_TYPE);
            push = ConsumeType.valueOf(property) == ConsumeType.CONSUME_PASSIVELY;
        }

        boolean startForAWhile = false;
        {
            String property =
                    prev.getProperties().getProperty(ConsumerRunningInfo.PROP_CONSUMER_START_TIMESTAMP);
            startForAWhile = (System.currentTimeMillis() - Long.parseLong(property)) > (1000 * 60 * 2);
        }

        // 只检测PUSH
        if (push && startForAWhile) {
            // 分析订阅关系是否相同
            {
                Iterator<Entry<String, ConsumerRunningInfo>> it = criTable.entrySet().iterator();
                while (it.hasNext()) {
                    Entry<String, ConsumerRunningInfo> next = it.next();
                    ConsumerRunningInfo current = next.getValue();
                    boolean equals = current.getSubscriptionSet().equals(prev.getSubscriptionSet());
                    // 发现订阅关系有误
                    if (!equals) {
                        // Different subscription in the same group of consumer
                        return false;
                    }

                    prev = next.getValue();
                }

                if (prev != null) {
                    // 无订阅关系
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

                // 顺序消息
                if (orderMsg) {
                    // 没锁住
                    if (!pq.isLocked()) {
                        sb.append(String.format("%s %s can't lock for a while, %dms\n", //
                            clientId,//
                            mq,//
                            System.currentTimeMillis() - pq.getLastLockTimestamp()));
                    }
                    // 锁住
                    else {
                        // Rebalance已经丢弃此队列，但是没有正常释放Lock
                        if (pq.isDroped() && (pq.getTryUnlockTimes() > 0)) {
                            sb.append(String.format("%s %s unlock %d times, still failed\n",//
                                clientId,//
                                mq,//
                                pq.getTryUnlockTimes()));
                        }
                    }

                    // 事务消息未提交
                }
                // 乱序消息
                else {
                    long diff = System.currentTimeMillis() - pq.getLastConsumeTimestamp();
                    // 在有消息的情况下，超过1分钟没再消费消息了
                    if (diff > (1000 * 60) && pq.getCachedMsgCount() > 0) {
                        sb.append(String.format("%s %s can't consume for a while, maybe blocked, %dms\n",//
                            clientId,//
                            mq, //
                            diff));
                    }
                }
            }
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
