package com.alibaba.rocketmq.tools.monitor;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;

import com.alibaba.rocketmq.client.consumer.DefaultMQPullConsumer;
import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.PullResult;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.client.exception.MQBrokerException;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.log.ClientLogger;
import com.alibaba.rocketmq.common.MQVersion;
import com.alibaba.rocketmq.common.MixAll;
import com.alibaba.rocketmq.common.ThreadFactoryImpl;
import com.alibaba.rocketmq.common.admin.ConsumeStats;
import com.alibaba.rocketmq.common.admin.OffsetWrapper;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.message.MessageQueue;
import com.alibaba.rocketmq.common.protocol.body.Connection;
import com.alibaba.rocketmq.common.protocol.body.ConsumerConnection;
import com.alibaba.rocketmq.common.protocol.body.ConsumerRunningInfo;
import com.alibaba.rocketmq.common.protocol.body.TopicList;
import com.alibaba.rocketmq.common.protocol.topic.OffsetMovedEvent;
import com.alibaba.rocketmq.remoting.RPCHook;
import com.alibaba.rocketmq.remoting.exception.RemotingException;
import com.alibaba.rocketmq.tools.admin.DefaultMQAdminExt;


public class MonitorService {
    private final Logger log = ClientLogger.getLog();
    private final ScheduledExecutorService scheduledExecutorService = Executors
        .newSingleThreadScheduledExecutor(new ThreadFactoryImpl("MonitorService"));

    private final MonitorConfig monitorConfig;

    private final MonitorListener monitorListener;

    private final DefaultMQAdminExt defaultMQAdminExt;
    private final DefaultMQPullConsumer defaultMQPullConsumer = new DefaultMQPullConsumer(
        MixAll.TOOLS_CONSUMER_GROUP);
    private final DefaultMQPushConsumer defaultMQPushConsumer = new DefaultMQPushConsumer(
        MixAll.MONITOR_CONSUMER_GROUP);


    public MonitorService(MonitorConfig monitorConfig, MonitorListener monitorListener, RPCHook rpcHook) {
        this.monitorConfig = monitorConfig;
        this.monitorListener = monitorListener;

        this.defaultMQAdminExt = new DefaultMQAdminExt(rpcHook);
        this.defaultMQAdminExt.setInstanceName(instanceName());
        this.defaultMQAdminExt.setNamesrvAddr(monitorConfig.getNamesrvAddr());

        this.defaultMQPullConsumer.setInstanceName(instanceName());
        this.defaultMQPullConsumer.setNamesrvAddr(monitorConfig.getNamesrvAddr());

        this.defaultMQPushConsumer.setInstanceName(instanceName());
        this.defaultMQPushConsumer.setNamesrvAddr(monitorConfig.getNamesrvAddr());
        try {
            this.defaultMQPushConsumer.setConsumeThreadMin(1);
            this.defaultMQPushConsumer.setConsumeThreadMax(1);
            this.defaultMQPushConsumer.subscribe(MixAll.OFFSET_MOVED_EVENT, "*");
            this.defaultMQPushConsumer.registerMessageListener(new MessageListenerConcurrently() {

                @Override
                public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
                        ConsumeConcurrentlyContext context) {
                    try {
                        OffsetMovedEvent ome =
                                OffsetMovedEvent.decode(msgs.get(0).getBody(), OffsetMovedEvent.class);

                        DeleteMsgsEvent deleteMsgsEvent = new DeleteMsgsEvent();
                        deleteMsgsEvent.setOffsetMovedEvent(ome);
                        deleteMsgsEvent.setEventTimestamp(msgs.get(0).getStoreTimestamp());

                        MonitorService.this.monitorListener.reportDeleteMsgsEvent(deleteMsgsEvent);
                    }
                    catch (Exception e) {
                    }

                    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                }
            });
        }
        catch (MQClientException e) {
        }
    }


    private String instanceName() {
        String name =
                System.currentTimeMillis() + new Random().nextInt() + this.monitorConfig.getNamesrvAddr();

        return "MonitorService_" + name.hashCode();
    }


    private void startScheduleTask() {
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    MonitorService.this.doMonitorWork();
                }
                catch (Exception e) {
                    log.error("doMonitorWork Exception", e);
                }
            }
        }, 1000 * 20, this.monitorConfig.getRoundInterval(), TimeUnit.MILLISECONDS);
    }


    public void start() throws MQClientException {
        this.defaultMQPullConsumer.start();
        this.defaultMQAdminExt.start();
        this.defaultMQPushConsumer.start();
        this.startScheduleTask();
    }


    public void shutdown() {
        this.defaultMQPullConsumer.shutdown();
        this.defaultMQAdminExt.shutdown();
        this.defaultMQPushConsumer.shutdown();
    }


    public void doMonitorWork() throws RemotingException, MQClientException, InterruptedException {
        long beginTime = System.currentTimeMillis();
        this.monitorListener.beginRound();

        TopicList topicList = defaultMQAdminExt.fetchAllTopicList();
        for (String topic : topicList.getTopicList()) {
            if (topic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
                String consumerGroup = topic.substring(MixAll.RETRY_GROUP_TOPIC_PREFIX.length());
                // 监控消费进度
                try {
                    this.reportUndoneMsgs(consumerGroup);
                }
                catch (Exception e) {
                    // log.error("reportUndoneMsgs Exception", e);
                }

                // 监控每个Consumer内存状态
                try {
                    this.reportConsumerRunningInfo(consumerGroup);
                }
                catch (Exception e) {
                    // log.error("reportConsumerRunningInfo Exception", e);
                }
            }
        }
        this.monitorListener.endRound();
        long spentTimeMills = System.currentTimeMillis() - beginTime;
        log.info("Execute one round monitor work, spent timemills: {}", spentTimeMills);
    }


    public void reportConsumerRunningInfo(final String consumerGroup) throws InterruptedException,
            MQBrokerException, RemotingException, MQClientException {
        ConsumerConnection cc = defaultMQAdminExt.examineConsumerConnectionInfo(consumerGroup);
        TreeMap<String, ConsumerRunningInfo> infoMap = new TreeMap<String, ConsumerRunningInfo>();
        for (Connection c : cc.getConnectionSet()) {
            String clientId = c.getClientId();
            // 低于3.1.8版本，不支持此功能
            if (c.getVersion() < MQVersion.Version.V3_1_8_SNAPSHOT.ordinal()) {
                continue;
            }

            try {
                ConsumerRunningInfo info =
                        defaultMQAdminExt.getConsumerRunningInfo(consumerGroup, clientId, false);
                infoMap.put(clientId, info);
            }
            catch (Exception e) {
            }
        }

        if (!infoMap.isEmpty()) {
            this.monitorListener.reportConsumerRunningInfo(infoMap);
        }
    }


    private void reportFailedMsgs(final String consumerGroup, final String topic) {

    }


    private void reportUndoneMsgs(final String consumerGroup) {
        ConsumeStats cs = null;
        try {
            cs = defaultMQAdminExt.examineConsumeStats(consumerGroup);
        }
        catch (Exception e) {
            return;
        }

        ConsumerConnection cc = null;
        try {
            cc = defaultMQAdminExt.examineConsumerConnectionInfo(consumerGroup);
        }
        catch (Exception e) {
            return;
        }

        if (cs != null) {
            // 按照Topic拆分
            HashMap<String/* Topic */, ConsumeStats> csByTopic = new HashMap<String, ConsumeStats>();
            {
                Iterator<Entry<MessageQueue, OffsetWrapper>> it = cs.getOffsetTable().entrySet().iterator();
                while (it.hasNext()) {
                    Entry<MessageQueue, OffsetWrapper> next = it.next();
                    MessageQueue mq = next.getKey();
                    OffsetWrapper ow = next.getValue();
                    ConsumeStats csTmp = csByTopic.get(mq.getTopic());
                    if (null == csTmp) {
                        csTmp = new ConsumeStats();
                        csByTopic.put(mq.getTopic(), csTmp);
                    }

                    csTmp.getOffsetTable().put(mq, ow);
                }
            }

            // 按照Topic开始报警
            {
                Iterator<Entry<String, ConsumeStats>> it = csByTopic.entrySet().iterator();
                while (it.hasNext()) {
                    Entry<String, ConsumeStats> next = it.next();
                    UndoneMsgs undoneMsgs = new UndoneMsgs();
                    undoneMsgs.setConsumerGroup(consumerGroup);
                    undoneMsgs.setTopic(next.getKey());
                    this.computeUndoneMsgs(undoneMsgs, next.getValue());
                    this.monitorListener.reportUndoneMsgs(undoneMsgs);
                    this.reportFailedMsgs(consumerGroup, next.getKey());
                }
            }
        }
    }


    private void computeUndoneMsgs(final UndoneMsgs undoneMsgs, final ConsumeStats consumeStats) {
        long total = 0;
        long singleMax = 0;
        long delayMax = 0;
        Iterator<Entry<MessageQueue, OffsetWrapper>> it = consumeStats.getOffsetTable().entrySet().iterator();
        while (it.hasNext()) {
            Entry<MessageQueue, OffsetWrapper> next = it.next();
            MessageQueue mq = next.getKey();
            OffsetWrapper ow = next.getValue();
            long diff = ow.getBrokerOffset() - ow.getConsumerOffset();

            if (diff > singleMax) {
                singleMax = diff;
            }

            if (diff > 0) {
                total += diff;
            }

            // Delay
            if (ow.getLastTimestamp() > 0) {
                try {
                    long maxOffset = this.defaultMQPullConsumer.maxOffset(mq);
                    if (maxOffset > 0) {
                        PullResult pull = this.defaultMQPullConsumer.pull(mq, "*", maxOffset - 1, 1);
                        switch (pull.getPullStatus()) {
                        case FOUND:
                            long delay =
                                    pull.getMsgFoundList().get(0).getStoreTimestamp() - ow.getLastTimestamp();
                            if (delay > delayMax) {
                                delayMax = delay;
                            }
                            break;
                        case NO_MATCHED_MSG:
                        case NO_NEW_MSG:
                        case OFFSET_ILLEGAL:
                            break;
                        default:
                            break;
                        }
                    }
                }
                catch (Exception e) {
                }
            }
        }

        undoneMsgs.setUndoneMsgsTotal(total);
        undoneMsgs.setUndoneMsgsSingleMQ(singleMax);
        undoneMsgs.setUndoneMsgsDelayTimeMills(delayMax);
    }


    public static void main(String[] args) throws MQClientException {
        main0(args, null);
    }


    public static void main0(String[] args, RPCHook rpcHook) throws MQClientException {
        final MonitorService monitorService =
                new MonitorService(new MonitorConfig(), new DefaultMonitorListener(), rpcHook);
        monitorService.start();

        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            private volatile boolean hasShutdown = false;


            @Override
            public void run() {
                synchronized (this) {
                    if (!this.hasShutdown) {
                        this.hasShutdown = true;
                        monitorService.shutdown();
                    }
                }
            }
        }, "ShutdownHook"));
    }
}
