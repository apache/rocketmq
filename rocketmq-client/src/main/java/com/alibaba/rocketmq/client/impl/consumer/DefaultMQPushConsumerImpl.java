/**
 * $Id: DefaultMQPushConsumerImpl.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.client.impl.consumer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;

import com.alibaba.rocketmq.client.QueryResult;
import com.alibaba.rocketmq.client.consumer.AllocateMessageQueueStrategy;
import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.MQPushConsumer;
import com.alibaba.rocketmq.client.consumer.PullCallback;
import com.alibaba.rocketmq.client.consumer.PullResult;
import com.alibaba.rocketmq.client.consumer.listener.MessageListener;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerOrderly;
import com.alibaba.rocketmq.client.consumer.store.LocalFileOffsetStore;
import com.alibaba.rocketmq.client.consumer.store.OffsetStore;
import com.alibaba.rocketmq.client.consumer.store.RemoteBrokerOffsetStore;
import com.alibaba.rocketmq.client.exception.MQBrokerException;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.impl.CommunicationMode;
import com.alibaba.rocketmq.client.impl.MQClientManager;
import com.alibaba.rocketmq.client.impl.factory.MQClientFactory;
import com.alibaba.rocketmq.client.log.ClientLogger;
import com.alibaba.rocketmq.common.ServiceState;
import com.alibaba.rocketmq.common.TopicFilterType;
import com.alibaba.rocketmq.common.help.FAQUrl;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.message.MessageQueue;
import com.alibaba.rocketmq.common.protocol.heartbeat.ConsumeType;
import com.alibaba.rocketmq.common.protocol.heartbeat.MessageModel;
import com.alibaba.rocketmq.common.protocol.heartbeat.SubscriptionData;
import com.alibaba.rocketmq.common.sysflag.PullSysFlag;
import com.alibaba.rocketmq.remoting.exception.RemotingException;


/**
 * @author shijia.wxr<vintage.wang@gmail.com>
 */
public class DefaultMQPushConsumerImpl implements MQPushConsumer, MQConsumerInner {
    private final Logger log = ClientLogger.getLog();
    private final DefaultMQPushConsumer defaultMQPushConsumer;
    private ServiceState serviceState = ServiceState.CREATE_JUST;
    private MQClientFactory mQClientFactory;
    private PullAPIWrapper pullAPIWrapper;

    private volatile boolean pause = false;

    // 消费进度存储
    private OffsetStore offsetStore;

    // 消息存储相关
    private ConcurrentHashMap<MessageQueue, ProcessQueue> processQueueTable =
            new ConcurrentHashMap<MessageQueue, ProcessQueue>(64);

    // 消费消息服务
    private ConsumeMessageService consumeMessageService;

    // 可订阅的信息（定时从Name Server更新最新版本）
    private final ConcurrentHashMap<String/* topic */, Set<MessageQueue>> topicSubscribeInfoTable =
            new ConcurrentHashMap<String, Set<MessageQueue>>();

    // 是否顺序消费消息
    private boolean consumeOrderly = false;


    @Override
    public void start() throws MQClientException {
        switch (this.serviceState) {
        case CREATE_JUST:
            this.serviceState = ServiceState.RUNNING;

            this.mQClientFactory =
                    MQClientManager.getInstance().getAndCreateMQClientFactory(this.defaultMQPushConsumer);

            this.pullAPIWrapper = new PullAPIWrapper(//
                mQClientFactory,//
                this.defaultMQPushConsumer.getConsumerGroup(),//
                this.defaultMQPushConsumer.getConsumeFromWhichNode());

            // 广播消费/集群消费
            switch (this.defaultMQPushConsumer.getMessageModel()) {
            case BROADCASTING:
                this.offsetStore =
                        new LocalFileOffsetStore(this.mQClientFactory,
                            this.defaultMQPushConsumer.getConsumerGroup());
                break;
            case CLUSTERING:
                this.offsetStore =
                        new RemoteBrokerOffsetStore(this.mQClientFactory,
                            this.defaultMQPushConsumer.getConsumerGroup());
                break;
            default:
                break;
            }

            // 加载消费进度
            this.offsetStore.load();

            // 启动消费消息服务
            if (this.defaultMQPushConsumer.getMessageListener() instanceof MessageListenerOrderly) {
                this.consumeOrderly = true;
                this.consumeMessageService =
                        new ConsumeMessageOrderlyService(this,
                            (MessageListenerOrderly) this.defaultMQPushConsumer.getMessageListener());
            }
            else if (this.defaultMQPushConsumer.getMessageListener() instanceof MessageListenerConcurrently) {
                this.consumeOrderly = false;
                this.consumeMessageService =
                        new ConsumeMessageConcurrentlyService(this,
                            (MessageListenerConcurrently) this.defaultMQPushConsumer.getMessageListener());
            }
            else {
                throw new MQClientException("the message listener is wrong", null);
            }

            this.consumeMessageService.start();

            boolean registerOK =
                    mQClientFactory.registerConsumer(this.defaultMQPushConsumer.getConsumerGroup(), this);
            if (!registerOK) {
                this.serviceState = ServiceState.CREATE_JUST;
                throw new MQClientException("The consumer group[" + this.defaultMQPushConsumer.getConsumerGroup()
                        + "] has created already, specifed another name please."
                        + FAQUrl.suggestTodo(FAQUrl.GROUP_NAME_DUPLICATE_URL), null);
            }

            mQClientFactory.start();
            log.info("the consumer [{}] start OK", this.defaultMQPushConsumer.getConsumerGroup());
            break;
        case RUNNING:
            break;
        case SHUTDOWN_ALREADY:
            break;
        default:
            break;
        }

        this.updateTopicSubscribeInfoWhenSubscriptionChanged();

        this.mQClientFactory.sendHeartbeatToAllBrokerWithLock();

        this.mQClientFactory.rebalanceImmediately();
    }


    @Override
    public void shutdown() {
        switch (this.serviceState) {
        case CREATE_JUST:
            break;
        case RUNNING:
            this.serviceState = ServiceState.SHUTDOWN_ALREADY;
            // TODO 存储消费进度时，需要考虑集群模式覆盖掉其他消费者消费进度的问题
            this.consumeMessageService.shutdown();
            this.persistConsumerOffset();
            this.mQClientFactory.unregisterConsumer(this.defaultMQPushConsumer.getConsumerGroup());
            this.mQClientFactory.shutdown();
            log.info("the consumer [{}] shutdown OK", this.defaultMQPushConsumer.getConsumerGroup());
            break;
        case SHUTDOWN_ALREADY:
            break;
        default:
            break;
        }
    }


    private void makeSureStateOK() throws MQClientException {
        if (this.serviceState != ServiceState.RUNNING) {
            throw new MQClientException("The consumer service state not OK, " + this.serviceState, null);
        }
    }


    public DefaultMQPushConsumerImpl(DefaultMQPushConsumer defaultMQPushConsumer) {
        this.defaultMQPushConsumer = defaultMQPushConsumer;
    }


    public void updateConsumeOffset(MessageQueue mq, long offset) {
        this.offsetStore.updateOffset(mq, offset);
    }


    @Override
    public void sendMessageBack(MessageExt msg, MessageQueue mq, int delayLevel) {
        // TODO Auto-generated method stub
    }


    @Override
    public Set<MessageQueue> fetchSubscribeMessageQueues(String topic) throws MQClientException {
        return this.topicSubscribeInfoTable.get(topic);
    }


    @Override
    public void createTopic(String key, String newTopic, int queueNum, TopicFilterType topicFilterType,
            boolean order) throws MQClientException {
        this.mQClientFactory.getMQAdminImpl().createTopic(key, newTopic, queueNum, topicFilterType, order);
    }


    @Override
    public long searchOffset(MessageQueue mq, long timestamp) throws MQClientException {
        return this.mQClientFactory.getMQAdminImpl().searchOffset(mq, timestamp);
    }


    @Override
    public long getMaxOffset(MessageQueue mq) throws MQClientException {
        return this.mQClientFactory.getMQAdminImpl().getMaxOffset(mq);
    }


    @Override
    public long getMinOffset(MessageQueue mq) throws MQClientException {
        return this.mQClientFactory.getMQAdminImpl().getMinOffset(mq);
    }


    @Override
    public long getEarliestMsgStoreTime(MessageQueue mq) throws MQClientException {
        return this.mQClientFactory.getMQAdminImpl().getEarliestMsgStoreTime(mq);
    }


    @Override
    public MessageExt viewMessage(String msgId) throws RemotingException, MQBrokerException, InterruptedException,
            MQClientException {
        return this.mQClientFactory.getMQAdminImpl().viewMessage(msgId);
    }


    @Override
    public QueryResult queryMessage(String topic, String key, int maxNum, long begin, long end)
            throws MQClientException, InterruptedException {
        return this.mQClientFactory.getMQAdminImpl().queryMessage(topic, key, maxNum, begin, end);
    }


    @Override
    public void registerMessageListener(MessageListener messageListener) {
        // TODO Auto-generated method stub

    }


    @Override
    public void subscribe(String topic, String subExpression) {
        // TODO Auto-generated method stub

    }


    @Override
    public void unsubscribe(String topic) {
        // TODO Auto-generated method stub
    }


    @Override
    public void suspend() {
        this.pause = true;
        log.info("suspend this consumer, {}", this.defaultMQPushConsumer.getConsumerGroup());
    }


    @Override
    public void resume() {
        this.pause = false;
        log.info("resume this consumer, {}", this.defaultMQPushConsumer.getConsumerGroup());
    }


    @Override
    public String getGroupName() {
        return this.defaultMQPushConsumer.getConsumerGroup();
    }


    @Override
    public MessageModel getMessageModel() {
        return this.defaultMQPushConsumer.getMessageModel();
    }


    @Override
    public ConsumeType getConsumeType() {
        return ConsumeType.CONSUME_PASSIVELY;
    }


    @Override
    public Set<SubscriptionData> getMQSubscriptions() {
        return null;
    }


    /**
     * 稍后再执行这个PullRequest
     */
    private void executePullRequestLater(final PullRequest pullRequest, final long timeDelay) {
        this.mQClientFactory.getPullMessageService().executePullRequestLater(pullRequest, timeDelay);
    }


    /**
     * 立刻执行这个PullRequest
     */
    public void executePullRequestImmediately(final PullRequest pullRequest) {
        this.mQClientFactory.getPullMessageService().executePullRequestImmediately(pullRequest);
    }

    private final long PullTimeDelayMillsWhenException = 3000;
    private final long PullTimeDelayMillsWhenFlowControl = 1000;
    private final long PullTimeDelayMillsWhenSuspend = 1000;


    public void pullMessage(final PullRequest pullRequest) {
        final ProcessQueue processQueue = pullRequest.getProcessQueue();
        if (processQueue.isDroped()) {
            log.info("the pull request[{}] is droped.", pullRequest.toString());
            return;
        }

        // 检测Consumer是否启动
        try {
            this.makeSureStateOK();
        }
        catch (MQClientException e) {
            log.warn("pullMessage exception, consumer state not ok", e);
            this.executePullRequestLater(pullRequest, PullTimeDelayMillsWhenException);
            return;
        }

        // 检测Consumer是否被挂起
        if (this.isPause()) {
            this.executePullRequestLater(pullRequest, PullTimeDelayMillsWhenSuspend);
            return;
        }

        // 流量控制，队列中消息总数
        long size = processQueue.getMsgCount().get();
        if (size > this.defaultMQPushConsumer.getPullThresholdForQueue()) {
            this.executePullRequestLater(pullRequest, PullTimeDelayMillsWhenFlowControl);
            log.warn("the consumer message buffer is full, so do flow control, {} {}", size, pullRequest);
            return;
        }

        // 流量控制，队列中消息最大跨度
        if (!this.consumeOrderly) {
            if (processQueue.getMaxSpan() > this.defaultMQPushConsumer.getConsumeConcurrentlyMaxSpan()) {
                this.executePullRequestLater(pullRequest, PullTimeDelayMillsWhenFlowControl);
                log.warn("the consumer message in the queue, span too long, so do flow control, {} {}", size,
                    pullRequest);
                return;
            }
        }

        PullCallback pullCallback = new PullCallback() {
            @Override
            public void onSuccess(PullResult pullResult) {
                if (pullResult != null) {
                    pullResult =
                            DefaultMQPushConsumerImpl.this.pullAPIWrapper.processPullResult(
                                pullRequest.getMessageQueue(), pullResult);

                    pullRequest.setNextOffset(pullResult.getNextBeginOffset());

                    switch (pullResult.getPullStatus()) {
                    case FOUND:
                        processQueue.putMessage(pullResult.getMsgFoundList());
                        DefaultMQPushConsumerImpl.this.consumeMessageService.submitConsumeRequest(
                            pullResult.getMsgFoundList(), processQueue, pullRequest.getMessageQueue());

                        // 流控
                        if (DefaultMQPushConsumerImpl.this.defaultMQPushConsumer.getPullInterval() > 0) {
                            DefaultMQPushConsumerImpl.this.executePullRequestLater(pullRequest,
                                DefaultMQPushConsumerImpl.this.defaultMQPushConsumer.getPullInterval());
                        }
                        // 立刻拉消息
                        else {
                            DefaultMQPushConsumerImpl.this.executePullRequestImmediately(pullRequest);
                        }

                        break;
                    case NO_NEW_MSG:
                        DefaultMQPushConsumerImpl.this.executePullRequestImmediately(pullRequest);
                        break;
                    case NO_MATCHED_MSG:
                        pullRequest.setNextOffset(pullResult.getNextBeginOffset());
                        DefaultMQPushConsumerImpl.this.executePullRequestImmediately(pullRequest);
                        break;
                    case OFFSET_ILLEGAL:
                        log.warn("the pull request offset illegal, {} {}",//
                            pullRequest.toString(), pullResult.toString());
                        if (pullRequest.getNextOffset() < pullResult.getMinOffset()) {
                            pullRequest.setNextOffset(pullResult.getMinOffset());
                        }
                        else if (pullRequest.getNextOffset() > pullResult.getMaxOffset()) {
                            pullRequest.setNextOffset(pullResult.getMaxOffset());
                        }

                        log.warn("fix the pull request offset, {}", pullRequest);
                        DefaultMQPushConsumerImpl.this.executePullRequestImmediately(pullRequest);
                        break;
                    default:
                        break;
                    }
                }
            }


            @Override
            public void onException(Throwable e) {
                log.warn("execute the pull request exception", e);
                DefaultMQPushConsumerImpl.this.executePullRequestLater(pullRequest,
                    PullTimeDelayMillsWhenException);
            }
        };

        int sysFlag = PullSysFlag.buildSysFlag(//
            false, // commitOffset
            true, // suspend
            false// subscription
            );
        try {
            this.pullAPIWrapper.pullKernelImpl(//
                pullRequest.getMessageQueue(), // 1
                null, // 2
                pullRequest.getNextOffset(), // 3
                this.defaultMQPushConsumer.getPullBatchSize(), // 4
                sysFlag, // 5
                0,// 6
                1000 * 10, // 7
                1000 * 20, // 8
                CommunicationMode.ASYNC, // 9
                pullCallback// 10
                );
        }
        catch (Exception e) {
            log.error("pullKernelImpl exception", e);
            this.executePullRequestLater(pullRequest, PullTimeDelayMillsWhenException);
        }
    }


    @Override
    public void persistConsumerOffset() {
        try {
            this.makeSureStateOK();
            Set<MessageQueue> mqs = new HashSet<MessageQueue>();
            Set<MessageQueue> allocateMq = this.processQueueTable.keySet();
            if (allocateMq != null) {
                mqs.addAll(allocateMq);
            }

            this.offsetStore.persistAll(mqs);
        }
        catch (Exception e) {
            log.error("group: " + this.defaultMQPushConsumer.getConsumerGroup()
                    + " persistConsumerOffset exception", e);
        }
    }


    public DefaultMQPushConsumer getDefaultMQPushConsumer() {
        return defaultMQPushConsumer;
    }


    private void updateProcessQueueTableInRebalance(final String topic, final Set<MessageQueue> mqSet) {
        // 将多余的队列删除
        for (MessageQueue mq : this.processQueueTable.keySet()) {
            if (mq.getTopic().equals(topic)) {
                if (!mqSet.contains(mq)) {
                    ProcessQueue pq = this.processQueueTable.remove(mq);
                    if (pq != null) {
                        pq.setDroped(true);
                        log.info("doRebalance, {}, remove unnecessary mq, {}",
                            this.defaultMQPushConsumer.getConsumerGroup(), mq);
                    }
                }
            }
        }

        // 增加新增的队列
        List<PullRequest> pullRequestList = new ArrayList<PullRequest>();
        for (MessageQueue mq : mqSet) {
            ProcessQueue pq = this.processQueueTable.get(mq);
            if (null == pq) {
                PullRequest pullRequest = new PullRequest();
                pullRequest.setConsumerGroup(this.defaultMQPushConsumer.getConsumerGroup());
                pullRequest.setMessageQueue(mq);
                pullRequest.setProcessQueue(new ProcessQueue());

                // TODO 这个需要根据策略来设置
                pullRequest.setNextOffset(0L);
                pullRequestList.add(pullRequest);
                this.processQueueTable.put(mq, pullRequest.getProcessQueue());
                log.info("doRebalance, {}, add a new mq, {}", this.defaultMQPushConsumer.getConsumerGroup(), mq);
            }
        }

        // 派发PullRequest
        for (PullRequest pullRequest : pullRequestList) {
            this.executePullRequestImmediately(pullRequest);
            log.info("doRebalance, {}, add a new pull request {}", this.defaultMQPushConsumer.getConsumerGroup(),
                pullRequest);
        }
    }


    private void rebalanceByTopic(final String topic) {
        switch (this.defaultMQPushConsumer.getMessageModel()) {
        case BROADCASTING: {
            Set<MessageQueue> mqSet = this.topicSubscribeInfoTable.get(topic);
            if (mqSet != null) {
                this.updateProcessQueueTableInRebalance(topic, mqSet);
            }
            else {
                log.warn("doRebalance, {}, but the topic[{}] not exist.",
                    this.defaultMQPushConsumer.getConsumerGroup(), topic);
            }
            break;
        }
        case CLUSTERING: {
            Set<MessageQueue> mqSet = this.topicSubscribeInfoTable.get(topic);
            List<String> cidAll =
                    this.mQClientFactory.findConsumerIdList(topic, this.defaultMQPushConsumer.getConsumerGroup());
            if (null == mqSet) {
                log.warn("doRebalance, {}, but the topic[{}] not exist.",
                    this.defaultMQPushConsumer.getConsumerGroup(), topic);
            }

            if (null == cidAll) {
                log.warn("doRebalance, {}, get consumer id list failed",
                    this.defaultMQPushConsumer.getConsumerGroup());
            }

            if (mqSet != null && cidAll != null) {
                List<MessageQueue> mqAll = new ArrayList<MessageQueue>();
                mqAll.addAll(mqSet);

                // 排序
                Collections.sort(mqAll);
                Collections.sort(cidAll);

                AllocateMessageQueueStrategy strategy =
                        this.defaultMQPushConsumer.getAllocateMessageQueueStrategy();

                // 执行分配算法
                List<MessageQueue> allocateResult =
                        strategy.allocate(this.mQClientFactory.getClientId(), mqAll, cidAll);

                Set<MessageQueue> allocateResultSet = new HashSet<MessageQueue>();
                if (allocateResult != null) {
                    allocateResultSet.addAll(allocateResult);
                }

                // 更新本地队列
                this.updateProcessQueueTableInRebalance(topic, allocateResultSet);
            }
            break;
        }
        default:
            break;
        }
    }


    private void truncateMessageQueueNotMyTopic() {
        Map<String, String> subTable = this.defaultMQPushConsumer.getSubscription();

        for (MessageQueue mq : this.processQueueTable.keySet()) {
            if (!subTable.containsKey(mq.getTopic())) {
                ProcessQueue pq = this.processQueueTable.remove(mq);
                if (pq != null) {
                    pq.setDroped(true);
                    log.info("doRebalance, {}, truncateMessageQueueNotMyTopic remove unnecessary mq, {}",
                        this.defaultMQPushConsumer.getConsumerGroup(), mq);
                }
            }
        }
    }


    @Override
    public void doRebalance() {
        Map<String, String> subTable = this.defaultMQPushConsumer.getSubscription();
        if (subTable != null) {
            for (final Map.Entry<String, String> entry : subTable.entrySet()) {
                final String topic = entry.getKey();
                try {
                    this.rebalanceByTopic(topic);
                }
                catch (Exception e) {
                    log.warn("rebalanceByTopic Exception", e);
                }
            }
        }

        this.truncateMessageQueueNotMyTopic();
    }


    private void updateTopicSubscribeInfoWhenSubscriptionChanged() {
        Map<String, String> subTable = this.defaultMQPushConsumer.getSubscription();
        if (subTable != null) {
            for (final Map.Entry<String, String> entry : subTable.entrySet()) {
                final String topic = entry.getKey();
                this.mQClientFactory.updateTopicRouteInfoFromNameServer(topic);
            }
        }
    }


    @Override
    public void updateTopicSubscribeInfo(String topic, Set<MessageQueue> info) {
        Map<String, String> subTable = this.defaultMQPushConsumer.getSubscription();
        if (subTable != null) {
            String value = subTable.get(topic);
            if (value != null) {
                this.topicSubscribeInfoTable.put(topic, info);
            }
        }
    }


    public boolean isPause() {
        return pause;
    }


    public void setPause(boolean pause) {
        this.pause = pause;
    }
}
