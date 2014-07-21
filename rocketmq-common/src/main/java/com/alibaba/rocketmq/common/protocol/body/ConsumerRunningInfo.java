package com.alibaba.rocketmq.common.protocol.body;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;

import com.alibaba.rocketmq.common.message.MessageQueue;
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

    // 各种配置及运行数据
    private Properties properties = new Properties();
    // 订阅关系
    private Set<SubscriptionData> subscriptionSet = new HashSet<SubscriptionData>();
    // 消费进度、Rebalance、内部消费队列的信息
    private TreeMap<MessageQueue, ProcessQueueInfo> mqTable = new TreeMap<MessageQueue, ProcessQueueInfo>();
    // RT、TPS统计
    private TreeMap<String/* Topic */, ConsumeStatus> statusTable = new TreeMap<String, ConsumeStatus>();


    public Properties getProperties() {
        return properties;
    }


    public void setProperties(Properties properties) {
        this.properties = properties;
    }


    public Set<SubscriptionData> getSubscriptionSet() {
        return subscriptionSet;
    }


    public void setSubscriptionSet(Set<SubscriptionData> subscriptionSet) {
        this.subscriptionSet = subscriptionSet;
    }


    public TreeMap<MessageQueue, ProcessQueueInfo> getMqTable() {
        return mqTable;
    }


    public void setMqTable(TreeMap<MessageQueue, ProcessQueueInfo> mqTable) {
        this.mqTable = mqTable;
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
                String item = String.format("%-32s  %14.2f %14.2f %14.2f %14.2f %18.2f %25.2f\n",//
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

        return sb.toString();
    }


    public TreeMap<String, ConsumeStatus> getStatusTable() {
        return statusTable;
    }


    public void setStatusTable(TreeMap<String, ConsumeStatus> statusTable) {
        this.statusTable = statusTable;
    }
}
