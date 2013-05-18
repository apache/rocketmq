/**
 * $Id: MQClientFactory.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.client.impl.factory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;

import com.alibaba.rocketmq.client.MQClientConfig;
import com.alibaba.rocketmq.client.consumer.ConsumeFromWhichNode;
import com.alibaba.rocketmq.client.exception.MQBrokerException;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.impl.FindBrokerResult;
import com.alibaba.rocketmq.client.impl.MQAdminImpl;
import com.alibaba.rocketmq.client.impl.MQClientAPIImpl;
import com.alibaba.rocketmq.client.impl.consumer.MQConsumerInner;
import com.alibaba.rocketmq.client.impl.producer.DefaultMQProducerImpl;
import com.alibaba.rocketmq.client.impl.producer.TopicPublishInfo;
import com.alibaba.rocketmq.common.MixAll;
import com.alibaba.rocketmq.common.MessageQueue;
import com.alibaba.rocketmq.common.ServiceState;
import com.alibaba.rocketmq.common.protocol.heartbeat.ConsumerData;
import com.alibaba.rocketmq.common.protocol.heartbeat.HeartbeatData;
import com.alibaba.rocketmq.common.protocol.heartbeat.ProducerData;
import com.alibaba.rocketmq.common.protocol.heartbeat.SubscriptionData;
import com.alibaba.rocketmq.common.protocol.route.BrokerData;
import com.alibaba.rocketmq.common.protocol.route.QueueData;
import com.alibaba.rocketmq.common.protocol.route.TopicRouteData;
import com.alibaba.rocketmq.remoting.exception.RemotingException;
import com.alibaba.rocketmq.remoting.netty.NettyClientConfig;
import com.google.protobuf.InvalidProtocolBufferException;


/**
 * 客户端Factory类，用来管理Producer与Consumer
 * 
 * @author vintage.wang@gmail.com shijia.wxr@taobao.com
 * 
 */
public class MQClientFactory {
    private ServiceState serviceState = ServiceState.CREATE_JUST;
    private final Logger log;
    private final MQClientConfig mQClientConfig;
    private final int factoryIndex;
    private final String clientId;
    private final long bootTimestamp = System.currentTimeMillis();

    private final ConcurrentHashMap<String/* group */, DefaultMQProducerImpl> producerTable =
            new ConcurrentHashMap<String, DefaultMQProducerImpl>();

    private final ConcurrentHashMap<String/* group */, MQConsumerInner> consumerTable =
            new ConcurrentHashMap<String, MQConsumerInner>();

    private final NettyClientConfig nettyClientConfig;
    private final MQClientAPIImpl mQClientAPIImpl;

    // 存储从Name Server拿到的Topic路由信息
    private final ConcurrentHashMap<String/* Topic */, TopicRouteData> topicRouteTable =
            new ConcurrentHashMap<String, TopicRouteData>();

    // 调用Name Server获取Topic路由信息时，加锁
    private final Lock lockNamesrv = new ReentrantLock();
    private final static long LockTimeoutMillis = 3000;

    // 存储Broker Name 与Broker Address的对应关系
    private final ConcurrentHashMap<String/* Broker Name */, HashMap<Long/* brokerId */, String/* address */>> brokerAddrTable =
            new ConcurrentHashMap<String, HashMap<Long, String>>();

    private ScheduledExecutorService scheduledExecutorService = Executors
        .newSingleThreadScheduledExecutor(new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, "MQClientFactoryScheduledThread");
            }
        });

    private final MQAdminImpl mQAdminImpl;


    public MQClientFactory(MQClientConfig mQClientConfig, int factoryIndex) {
        this.mQClientConfig = mQClientConfig;
        this.factoryIndex = factoryIndex;
        this.nettyClientConfig = new NettyClientConfig();
        this.nettyClientConfig.setClientCallbackExecutorThreads(mQClientConfig.getClientCallbackExecutorThreads());
        this.mQClientAPIImpl = new MQClientAPIImpl(this.nettyClientConfig);
        this.log = MixAll.createLogger(mQClientConfig.getLogFileName(), mQClientConfig.getLogLevel());

        if (this.mQClientConfig.getNamesrvAddr() != null) {
            this.mQClientAPIImpl.updateNameServerAddressList(this.mQClientConfig.getNamesrvAddr());
            log.info("user specfied name server address, " + this.mQClientConfig.getNamesrvAddr());
        }

        this.clientId = this.buildMQClientId();

        this.mQAdminImpl = new MQAdminImpl(this);

        log.info("created a new client fatory, " + this.factoryIndex);
    }


    private String buildMQClientId() {
        StringBuilder sb = new StringBuilder();
        sb.append(this.mQClientConfig.getClientIP());
        sb.append("@");

        sb.append(this.mQClientConfig.getInstanceName());
        sb.append("@");

        sb.append(MixAll.CURRENT_JVM_PID);
        sb.append("@");

        sb.append(this.bootTimestamp);
        return sb.toString();
    }


    private void startScheduledTask() {
        // 定时获取Name Server地址
        if (null == this.mQClientConfig.getNamesrvAddr()) {
            this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

                @Override
                public void run() {
                    try {
                        MQClientFactory.this.mQClientAPIImpl.fetchNameServerAddr();
                    }
                    catch (Exception e) {
                        log.error("ScheduledTask fetchNameServerAddr exception", e);
                    }
                }
            }, 1000 * 10, 1000 * 60 * 2, TimeUnit.MILLISECONDS);
        }

        // 定时从Name Server获取Topic路由信息
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

            @Override
            public void run() {
                try {
                    MQClientFactory.this.updateTopicRouteInfoFromNameServer();
                }
                catch (Exception e) {
                    log.error("ScheduledTask updateTopicRouteInfoFromNameServer exception", e);
                }
            }
        }, 0, this.mQClientConfig.getPollNameServerInteval(), TimeUnit.MILLISECONDS);

        // 向所有Broker发送心跳信息（包含订阅关系等）
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

            @Override
            public void run() {
                try {
                    MQClientFactory.this.sendHeartbeatToAllBroker();
                }
                catch (Exception e) {
                    log.error("ScheduledTask sendHeartbeatToAllBroker exception", e);
                }
            }
        }, 1000, this.mQClientConfig.getHeartbeatBrokerInterval(), TimeUnit.MILLISECONDS);
    }


    public void start() {
        synchronized (this) {
            switch (this.serviceState) {
            case CREATE_JUST:
                this.serviceState = ServiceState.RUNNING;
                // TODO
                if (null == this.mQClientConfig.getNamesrvAddr()) {
                    MQClientFactory.this.mQClientAPIImpl.fetchNameServerAddr();
                }

                this.startScheduledTask();
                this.mQClientAPIImpl.start();
                break;
            case RUNNING:
                break;
            case SHUTDOWN_ALREADY:
                break;
            default:
                break;
            }
        }
    }


    public void shutdown() {
        // Producer
        if (!this.producerTable.isEmpty())
            return;

        // Consumer
        if (!this.consumerTable.isEmpty())
            return;

        synchronized (this) {
            switch (this.serviceState) {
            case CREATE_JUST:
                break;
            case RUNNING:
                this.serviceState = ServiceState.SHUTDOWN_ALREADY;
                // TODO
                this.scheduledExecutorService.shutdown();
                this.mQClientAPIImpl.shutdown();
                break;
            case SHUTDOWN_ALREADY:
                break;
            default:
                break;
            }
        }
    }


    private HeartbeatData prepareHeartbeatData() {
        HeartbeatData heartbeatData = new HeartbeatData();

        // clientID
        heartbeatData.setClientID(this.clientId);

        // Consumer
        for (String group : this.consumerTable.keySet()) {
            MQConsumerInner impl = this.consumerTable.get(group);
            if (impl != null) {
                ConsumerData consumerData = new ConsumerData();
                consumerData.setGroupName(impl.getGroupName());
                consumerData.setConsumeType(impl.getConsumeType());
                consumerData.setMessageModel(impl.getMessageModel());
                consumerData.getSubscriptionDataSet().addAll(impl.getMQSubscriptions());

                heartbeatData.getConsumerDataSet().add(consumerData);
            }
        }

        // Producer
        for (String group : this.producerTable.keySet()) {
            DefaultMQProducerImpl impl = this.producerTable.get(group);
            if (impl != null) {
                ProducerData producerData = new ProducerData();
                producerData.setGroupName(group);

                heartbeatData.getProducerDataSet().add(producerData);
            }
        }

        return heartbeatData;
    }


    private void sendHeartbeatToAllBroker() {
        final HeartbeatData heartbeatData = this.prepareHeartbeatData();
        final boolean producerEmpty = heartbeatData.getProducerDataSet().isEmpty();
        final boolean consumerEmpty = heartbeatData.getConsumerDataSet().isEmpty();
        if (producerEmpty && consumerEmpty) {
            log.warn("sending hearbeat, but no consumer and no producer");
            return;
        }

        for (String name : this.brokerAddrTable.keySet()) {
            final HashMap<Long, String> oneTable = this.brokerAddrTable.get(name);
            if (oneTable != null) {
                for (Long id : oneTable.keySet()) {
                    String addr = oneTable.get(id);
                    if (addr != null) {
                        // 说明只有Producer，则不向Slave发心跳
                        if (consumerEmpty) {
                            if (id != MixAll.MASTER_ID)
                                continue;
                        }

                        try {
                            this.mQClientAPIImpl.sendHearbeat(addr, heartbeatData, 3000);
                            log.debug("send heart beat to broker[{} {} {}] success", name, id, addr);
                            log.debug(heartbeatData.toString());
                        }
                        catch (RemotingException e) {
                            log.error("send heart beat to broker exception", e);
                        }
                        catch (MQBrokerException e) {
                            log.error("send heart beat to broker exception", e);
                        }
                        catch (InterruptedException e) {
                            log.error("send heart beat to broker exception", e);
                        }
                    }
                }
            }
        }
    }


    public boolean registerProducer(final String group, final DefaultMQProducerImpl producer) {
        if (null == group || null == producer) {
            return false;
        }

        DefaultMQProducerImpl prev = this.producerTable.putIfAbsent(group, producer);
        if (prev != null) {
            log.warn("the producer group[{}] exist already.", group);
            return false;
        }

        return true;
    }


    public void unregisterProducer(final String group) {
        this.producerTable.remove(group);
    }


    public boolean registerConsumer(final String group, final MQConsumerInner consumer) {
        if (null == group || null == consumer) {
            return false;
        }

        MQConsumerInner prev = this.consumerTable.putIfAbsent(group, consumer);
        if (prev != null) {
            log.warn("the consumer group[" + group + "] exist already.");
            return false;
        }

        return true;
    }


    public void unregisterConsumer(final String group) {
        this.consumerTable.remove(group);
    }


    /**
     * 管理类的接口查询Broker地址，Master优先
     * 
     * @param brokerName
     * @return
     */
    public FindBrokerResult findBrokerAddressInAdmin(final String brokerName) {
        String brokerAddr = null;
        boolean slave = false;
        boolean found = false;

        HashMap<Long/* brokerId */, String/* address */> map = this.brokerAddrTable.get(brokerName);
        if (map != null && !map.isEmpty()) {
            // TODO 如果有多个Slave，可能会每次都选中相同的Slave，这里需要优化
            FOR_SEG: for (Map.Entry<Long, String> entry : map.entrySet()) {
                Long id = entry.getKey();
                brokerAddr = entry.getValue();
                if (brokerAddr != null) {
                    found = true;
                    if (MixAll.MASTER_ID == id) {
                        slave = false;
                        break FOR_SEG;
                    }
                    else {
                        slave = true;
                    }
                    break;

                }
            } // end of for
        }

        if (found) {
            return new FindBrokerResult(brokerAddr, slave);
        }

        return null;
    }


    /**
     * 发布消息过程中，寻找Broker地址，一定是找Master
     */
    public String findBrokerAddressInPublish(final String brokerName) {
        HashMap<Long/* brokerId */, String/* address */> map = this.brokerAddrTable.get(brokerName);
        if (map != null && !map.isEmpty()) {
            return map.get(MixAll.MASTER_ID);
        }

        return null;
    }


    /**
     * 订阅消息过程中，寻找Broker地址，取Master还是Slave由参数决定
     */
    public FindBrokerResult findBrokerAddressInSubscribe(//
            final String brokerName,//
            final ConsumeFromWhichNode consumeFromWhichNode//
    ) {
        String brokerAddr = null;
        boolean slave = false;
        boolean found = false;

        HashMap<Long/* brokerId */, String/* address */> map = this.brokerAddrTable.get(brokerName);
        if (map != null && !map.isEmpty()) {
            // TODO 如果有多个Slave，可能会每次都选中相同的Slave，这里需要优化
            FOR_SEG: for (Map.Entry<Long, String> entry : map.entrySet()) {
                Long id = entry.getKey();
                brokerAddr = entry.getValue();
                if (brokerAddr != null) {
                    switch (consumeFromWhichNode) {
                    case CONSUME_FROM_MASTER_FIRST:
                        found = true;
                        if (MixAll.MASTER_ID == id) {
                            slave = false;
                            break FOR_SEG;
                        }
                        else {
                            slave = true;
                        }
                        break;
                    case CONSUME_FROM_SLAVE_FIRST:
                        found = true;
                        if (MixAll.MASTER_ID != id) {
                            slave = true;
                            break FOR_SEG;
                        }
                        else {
                            slave = false;
                        }
                        break;
                    case CONSUME_FROM_MASTER_ONLY:
                        if (MixAll.MASTER_ID == id) {
                            found = true;
                            slave = false;
                            break FOR_SEG;
                        }
                        break;
                    case CONSUME_FROM_SLAVE_ONLY:
                        if (MixAll.MASTER_ID != id) {
                            found = true;
                            slave = true;
                            break FOR_SEG;
                        }
                        break;
                    default:
                        break;
                    }
                }
            } // end of for
        }

        if (found) {
            return new FindBrokerResult(brokerAddr, slave);
        }

        return null;
    }


    public static TopicPublishInfo topicRouteData2TopicPublishInfo(final String topic, final TopicRouteData route) {
        TopicPublishInfo info = new TopicPublishInfo();
        // 顺序消息
        if (route.getOrderTopicConf() != null && route.getOrderTopicConf().length() > 0) {
            String[] brokers = route.getOrderTopicConf().split(";");
            for (String broker : brokers) {
                String[] item = broker.split(":");
                int nums = Integer.parseInt(item[1]);
                for (int i = 0; i < nums; i++) {
                    MessageQueue mq = new MessageQueue(topic, item[0], i);
                    info.getMetaQueueList().add(mq);
                }
            }

            info.setOrderTopic(true);
        }
        // 非顺序消息
        else {
            List<QueueData> qds = route.getQueueDatas();
            // 排序原因：即使没有配置顺序消息模式，默认队列的顺序同配置的一致。
            Collections.sort(qds);
            for (QueueData qd : qds) {
                if (MixAll.isWriteable(qd.getPerm())) {
                    for (int i = 0; i < qd.getWriteQueueNums(); i++) {
                        MessageQueue mq = new MessageQueue(topic, qd.getBrokerName(), i);
                        info.getMetaQueueList().add(mq);
                    }
                }
            }

            info.setOrderTopic(false);
        }

        return info;
    }


    public static List<MessageQueue> topicRouteData2TopicSubscribeInfo(final String topic,
            final TopicRouteData route) {
        List<MessageQueue> mqList = new ArrayList<MessageQueue>();
        List<QueueData> qds = route.getQueueDatas();
        for (QueueData qd : qds) {
            if (MixAll.isReadable(qd.getPerm())) {
                for (int i = 0; i < qd.getWriteQueueNums(); i++) {
                    MessageQueue mq = new MessageQueue(topic, qd.getBrokerName(), i);
                    mqList.add(mq);
                }
            }
        }

        return mqList;
    }


    private void updateTopicRouteInfoFromNameServer() {
        Set<String> topicList = new HashSet<String>();

        // Consumer
        for (String g : this.consumerTable.keySet()) {
            MQConsumerInner impl = this.consumerTable.get(g);
            if (impl != null) {
                Set<SubscriptionData> subList = impl.getMQSubscriptions();
                for (SubscriptionData subData : subList) {
                    topicList.add(subData.getTopic());
                }
            }
        }

        // Producer
        for (String g : this.producerTable.keySet()) {
            DefaultMQProducerImpl impl = this.producerTable.get(g);
            if (impl != null) {
                Set<String> lst = impl.getPublishTopicList();
                topicList.addAll(lst);
            }
        }

        for (String topic : topicList) {
            this.updateTopicRouteInfoFromNameServer(topic);
        }
    }


    public TopicRouteData getAnExistTopicRouteData(final String topic) {
        return this.topicRouteTable.get(topic);
    }


    /**
     * 调用Name Server接口，根据Topic获取路由信息
     */
    public boolean updateTopicRouteInfoFromNameServer(final String topic) {
        try {
            if (this.lockNamesrv.tryLock(LockTimeoutMillis, TimeUnit.MILLISECONDS)) {
                try {
                    TopicRouteData topicRouteData =
                            this.mQClientAPIImpl.getTopicRouteInfoFromNameServer(topic, 1000 * 3);
                    if (topicRouteData != null) {
                        TopicRouteData old = this.topicRouteTable.get(topic);
                        if (null == old || !old.equals(topicRouteData)) {
                            log.info("the topic[" + topic + "] route info changed, " + topicRouteData);
                            // 更新Broker地址信息
                            for (BrokerData bd : topicRouteData.getBrokerDatas()) {
                                this.brokerAddrTable.put(bd.getBrokerName(), bd.getBrokerAddrs());
                            }

                            // 更新发布队列信息
                            TopicPublishInfo publishInfo = topicRouteData2TopicPublishInfo(topic, topicRouteData);
                            for (String g : this.producerTable.keySet()) {
                                DefaultMQProducerImpl impl = this.producerTable.get(g);
                                if (impl != null) {
                                    impl.updateTopicPublishInfo(topic, publishInfo);
                                }
                            }

                            // 更新订阅队列信息
                            // TODO

                            this.topicRouteTable.put(topic, topicRouteData);
                            return true;
                        }
                    }
                }
                catch (RemotingException e) {
                    log.warn("", e);
                }
                catch (MQClientException e) {
                    log.warn("", e);
                }
                catch (InterruptedException e) {
                    log.warn("", e);
                }
                catch (InvalidProtocolBufferException e) {
                    log.warn("", e);
                }
                finally {
                    this.lockNamesrv.unlock();
                }
            }
        }
        catch (InterruptedException e) {
            log.warn("", e);
        }

        return false;
    }


    public MQClientAPIImpl getMetaClientAPIImpl() {
        return mQClientAPIImpl;
    }


    public MQAdminImpl getMetaAdminImpl() {
        return mQAdminImpl;
    }


    public String getClientId() {
        return clientId;
    }
}
