/**
 * $Id: DefaultMQPullConsumerImpl.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.client.impl.consumer;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;

import com.alibaba.rocketmq.client.QueryResult;
import com.alibaba.rocketmq.client.consumer.DefaultMQPullConsumer;
import com.alibaba.rocketmq.client.consumer.PullCallback;
import com.alibaba.rocketmq.client.consumer.PullResult;
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
public class DefaultMQPullConsumerImpl implements MQConsumerInner {
    private final Logger log = ClientLogger.getLog();
    private ServiceState serviceState = ServiceState.CREATE_JUST;
    private final DefaultMQPullConsumer defaultMQPullConsumer;
    private MQClientFactory mQClientFactory;
    private PullAPIWrapper pullAPIWrapper;

    // 消费进度存储
    private OffsetStore offsetStore;


    public DefaultMQPullConsumerImpl(final DefaultMQPullConsumer defaultMQPullConsumer) {
        this.defaultMQPullConsumer = defaultMQPullConsumer;
    }


    public void start() throws MQClientException {
        switch (this.serviceState) {
        case CREATE_JUST:
            this.serviceState = ServiceState.RUNNING;

            this.mQClientFactory =
                    MQClientManager.getInstance().getAndCreateMQClientFactory(this.defaultMQPullConsumer);

            this.pullAPIWrapper = new PullAPIWrapper(//
                mQClientFactory,//
                this.defaultMQPullConsumer.getConsumerGroup(),//
                this.defaultMQPullConsumer.getConsumeFromWhichNode());

            // 广播消费/集群消费
            switch (this.defaultMQPullConsumer.getMessageModel()) {
            case BROADCASTING:
                this.offsetStore =
                        new LocalFileOffsetStore(this.mQClientFactory,
                            this.defaultMQPullConsumer.getConsumerGroup());
                break;
            case CLUSTERING:
                this.offsetStore =
                        new RemoteBrokerOffsetStore(this.mQClientFactory,
                            this.defaultMQPullConsumer.getConsumerGroup());
                break;
            case UNKNOWNS:
                break;
            default:
                break;
            }

            boolean registerOK =
                    mQClientFactory.registerConsumer(this.defaultMQPullConsumer.getConsumerGroup(), this);
            if (!registerOK) {
                this.serviceState = ServiceState.CREATE_JUST;
                throw new MQClientException("The consumer group[" + this.defaultMQPullConsumer.getConsumerGroup()
                        + "] has created already, specifed another name please."//
                        + FAQUrl.suggestTodo(FAQUrl.GROUP_NAME_DUPLICATE_URL), null);
            }

            mQClientFactory.start();
            log.info("the consumer [{}] start OK", this.defaultMQPullConsumer.getConsumerGroup());
            break;
        case RUNNING:
            break;
        case SHUTDOWN_ALREADY:
            break;
        default:
            break;
        }
    }


    public void shutdown() {
        switch (this.serviceState) {
        case CREATE_JUST:
            break;
        case RUNNING:
            this.serviceState = ServiceState.SHUTDOWN_ALREADY;
            this.persistConsumerOffset();
            this.mQClientFactory.unregisterConsumer(this.defaultMQPullConsumer.getConsumerGroup());
            this.mQClientFactory.shutdown();
            log.info("the consumer [{}] shutdown OK", this.defaultMQPullConsumer.getConsumerGroup());
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


    public void createTopic(String key, String newTopic, int queueNum, TopicFilterType topicFilterType,
            boolean order) throws MQClientException {
        this.makeSureStateOK();
        this.mQClientFactory.getMQAdminImpl().createTopic(key, newTopic, queueNum, topicFilterType, order);
    }


    public List<MessageQueue> fetchPublishMessageQueues(String topic) throws MQClientException {
        this.makeSureStateOK();
        return this.mQClientFactory.getMQAdminImpl().fetchPublishMessageQueues(topic);
    }


    public long searchOffset(MessageQueue mq, long timestamp) throws MQClientException {
        this.makeSureStateOK();
        return this.mQClientFactory.getMQAdminImpl().searchOffset(mq, timestamp);
    }


    public long getMaxOffset(MessageQueue mq) throws MQClientException {
        this.makeSureStateOK();
        return this.mQClientFactory.getMQAdminImpl().getMaxOffset(mq);
    }


    public long getMinOffset(MessageQueue mq) throws MQClientException {
        this.makeSureStateOK();
        return this.mQClientFactory.getMQAdminImpl().getMinOffset(mq);
    }


    public long getEarliestMsgStoreTime(MessageQueue mq) throws MQClientException {
        this.makeSureStateOK();
        return this.mQClientFactory.getMQAdminImpl().getEarliestMsgStoreTime(mq);
    }


    public MessageExt viewMessage(String msgId) throws RemotingException, MQBrokerException, InterruptedException,
            MQClientException {
        this.makeSureStateOK();
        return this.mQClientFactory.getMQAdminImpl().viewMessage(msgId);
    }


    public QueryResult queryMessage(String topic, String key, int maxNum, long begin, long end)
            throws MQClientException, InterruptedException {
        this.makeSureStateOK();
        return this.mQClientFactory.getMQAdminImpl().queryMessage(topic, key, maxNum, begin, end);
    }


    private PullResult pullSyncImpl(MessageQueue mq, String subExpression, long offset, int maxNums, boolean block)
            throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        this.makeSureStateOK();

        if (null == mq) {
            throw new MQClientException("mq is null", null);
        }

        if (offset < 0) {
            throw new MQClientException("offset < 0", null);
        }

        if (maxNums <= 0) {
            throw new MQClientException("maxNums <= 0", null);
        }

        int sysFlag = PullSysFlag.buildSysFlag(false, block, true);

        String subExpressionInner = subExpression != null ? subExpression : SubscriptionData.SUB_ALL;

        long timeoutMillis =
                block ? this.defaultMQPullConsumer.getConsumerTimeoutMillisWhenSuspend()
                        : this.defaultMQPullConsumer.getConsumerPullTimeoutMillis();

        PullResult pullResult =
                this.pullAPIWrapper.pullKernelImpl(mq, subExpressionInner, offset, maxNums, sysFlag, 0,
                    this.defaultMQPullConsumer.getBrokerSuspendMaxTimeMillis(), timeoutMillis,
                    CommunicationMode.SYNC, null);

        return this.pullAPIWrapper.processPullResult(mq, pullResult);
    }


    private void pullAsyncImpl(//
            final MessageQueue mq,//
            final String subExpression,//
            final long offset,//
            final int maxNums,//
            final PullCallback pullCallback,//
            final boolean block//
    ) throws MQClientException, RemotingException, InterruptedException {
        this.makeSureStateOK();

        if (null == mq) {
            throw new MQClientException("mq is null", null);
        }

        if (offset < 0) {
            throw new MQClientException("offset < 0", null);
        }

        if (maxNums <= 0) {
            throw new MQClientException("maxNums <= 0", null);
        }

        if (null == pullCallback) {
            throw new MQClientException("pullCallback is null", null);
        }

        try {

            int sysFlag = PullSysFlag.buildSysFlag(false, block, true);

            String subExpressionInner = subExpression != null ? subExpression : SubscriptionData.SUB_ALL;

            long timeoutMillis =
                    block ? this.defaultMQPullConsumer.getConsumerTimeoutMillisWhenSuspend()
                            : this.defaultMQPullConsumer.getConsumerPullTimeoutMillis();

            this.pullAPIWrapper.pullKernelImpl(mq, subExpressionInner, offset, maxNums, sysFlag, 0,
                this.defaultMQPullConsumer.getBrokerSuspendMaxTimeMillis(), timeoutMillis,
                CommunicationMode.ASYNC, new PullCallback() {

                    @Override
                    public void onSuccess(PullResult pullResult) {
                        pullCallback.onSuccess(DefaultMQPullConsumerImpl.this.pullAPIWrapper.processPullResult(mq,
                            pullResult));
                    }


                    @Override
                    public void onException(Throwable e) {
                        pullCallback.onException(e);
                    }
                });
        }
        catch (MQBrokerException e) {
            throw new MQClientException("pullAsync unknow exception", e);
        }
    }


    public PullResult pull(MessageQueue mq, String subExpression, long offset, int maxNums)
            throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return this.pullSyncImpl(mq, subExpression, offset, maxNums, false);
    }


    public PullResult pullBlockIfNotFound(MessageQueue mq, String subExpression, long offset, int maxNums)
            throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return this.pullSyncImpl(mq, subExpression, offset, maxNums, true);
    }


    public void pull(MessageQueue mq, String subExpression, long offset, int maxNums, PullCallback pullCallback)
            throws MQClientException, RemotingException, InterruptedException {
        this.pullAsyncImpl(mq, subExpression, offset, maxNums, pullCallback, false);
    }


    public void pullBlockIfNotFound(MessageQueue mq, String subExpression, long offset, int maxNums,
            PullCallback pullCallback) throws MQClientException, RemotingException, InterruptedException {
        this.pullAsyncImpl(mq, subExpression, offset, maxNums, pullCallback, true);
    }


    public void updateConsumeOffset(MessageQueue mq, long offset) throws MQClientException {
        this.makeSureStateOK();
        this.offsetStore.updateOffset(mq, offset);
    }


    public long fetchConsumeOffset(MessageQueue mq, boolean fromStore) throws MQClientException {
        this.makeSureStateOK();
        return this.offsetStore.readOffset(mq, fromStore);
    }


    public List<MessageQueue> fetchMessageQueuesInBalance(String topic) {
        // TODO Auto-generated method stub
        return null;
    }


    public List<MessageQueue> fetchSubscribeMessageQueues(String topic) throws MQClientException {
        return this.mQClientFactory.getMQAdminImpl().fetchSubscribeMessageQueues(topic);
    }


    @Override
    public MessageModel getMessageModel() {
        return MessageModel.UNKNOWNS;
    }


    @Override
    public ConsumeType getConsumeType() {
        return ConsumeType.CONSUME_ACTIVELY;
    }


    @Override
    public Set<SubscriptionData> getMQSubscriptions() {
        Set<SubscriptionData> result = new HashSet<SubscriptionData>();

        Set<String> topics = this.defaultMQPullConsumer.getRegisterTopics();
        if (topics != null) {
            synchronized (topics) {
                for (String t : topics) {
                    // TODO
                    SubscriptionData ms = new SubscriptionData(t, null, null, true);
                    result.add(ms);
                }
            }
        }

        return result;
    }


    @Override
    public String getGroupName() {
        return this.defaultMQPullConsumer.getConsumerGroup();
    }


    @Override
    public void persistConsumerOffset() {
        try {
            this.makeSureStateOK();
            this.offsetStore.persistAll();
        }
        catch (Exception e) {
            log.error("group: " + this.defaultMQPullConsumer.getConsumerGroup()
                    + " persistConsumerOffset exception", e);
        }
    }
}
