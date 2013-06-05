/**
 * $Id: DefaultMQPullConsumerImpl.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.client.impl.consumer;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import com.alibaba.rocketmq.client.QueryResult;
import com.alibaba.rocketmq.client.consumer.DefaultMQPullConsumer;
import com.alibaba.rocketmq.client.consumer.PullCallback;
import com.alibaba.rocketmq.client.consumer.PullResult;
import com.alibaba.rocketmq.client.consumer.PullStatus;
import com.alibaba.rocketmq.client.exception.MQBrokerException;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.impl.CommunicationMode;
import com.alibaba.rocketmq.client.impl.FindBrokerResult;
import com.alibaba.rocketmq.client.impl.MQClientManager;
import com.alibaba.rocketmq.client.impl.factory.MQClientFactory;
import com.alibaba.rocketmq.common.MessageDecoder;
import com.alibaba.rocketmq.common.MessageExt;
import com.alibaba.rocketmq.common.MessageQueue;
import com.alibaba.rocketmq.common.ServiceState;
import com.alibaba.rocketmq.common.TopicFilterType;
import com.alibaba.rocketmq.common.protocol.header.QueryConsumerOffsetRequestHeader;
import com.alibaba.rocketmq.common.protocol.header.UpdateConsumerOffsetRequestHeader;
import com.alibaba.rocketmq.common.protocol.heartbeat.ConsumeType;
import com.alibaba.rocketmq.common.protocol.heartbeat.MessageModel;
import com.alibaba.rocketmq.common.protocol.heartbeat.SubscriptionData;
import com.alibaba.rocketmq.common.sysflag.PullSysFlag;
import com.alibaba.rocketmq.remoting.exception.RemotingException;


/**
 * @author shijia.wxr<vintage.wang@gmail.com>
 */
public class DefaultMQPullConsumerImpl implements MQConsumerInner {
    private ServiceState serviceState = ServiceState.CREATE_JUST;
    private final DefaultMQPullConsumer defaultMQPullConsumer;
    private MQClientFactory mQClientFactory;
    private PullAPIWrapper pullAPIWrapper;

    private ConcurrentHashMap<MessageQueue, AtomicLong> offsetTable =
            new ConcurrentHashMap<MessageQueue, AtomicLong>();


    public DefaultMQPullConsumerImpl(final DefaultMQPullConsumer defaultMQPullConsumer) {
        this.defaultMQPullConsumer = defaultMQPullConsumer;
    }


    public void start() throws MQClientException {
        switch (this.serviceState) {
        case CREATE_JUST:
            this.serviceState = ServiceState.RUNNING;

            this.mQClientFactory =
                    MQClientManager.getInstance().getAndCreateMQClientFactory(
                        this.defaultMQPullConsumer.getClientConfig());

            this.pullAPIWrapper = new PullAPIWrapper(//
                mQClientFactory,//
                this.defaultMQPullConsumer.getConsumerGroup(),//
                this.defaultMQPullConsumer.getConsumeFromWhichNode());

            boolean registerOK =
                    mQClientFactory.registerConsumer(this.defaultMQPullConsumer.getConsumerGroup(), this);
            if (!registerOK) {
                this.serviceState = ServiceState.CREATE_JUST;
                throw new MQClientException("The consumer group[" + this.defaultMQPullConsumer.getConsumerGroup()
                        + "] has created already, specifed another name please.", null);
            }

            mQClientFactory.start();
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
            this.uploadConsumerOffsetsToBroker();
            this.mQClientFactory.unregisterConsumer(this.defaultMQPullConsumer.getConsumerGroup());
            this.mQClientFactory.shutdown();
            break;
        case SHUTDOWN_ALREADY:
            break;
        default:
            break;
        }
    }


    private void makeSureStateOK() throws MQClientException {
        if (this.serviceState != ServiceState.RUNNING) {
            throw new MQClientException("The producer service state not OK", null);
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

        return this.processPullResult(mq, pullResult);
    }


    /**
     * 对拉取结果进行处理，主要是消息反序列化
     */
    private PullResult processPullResult(final MessageQueue mq, final PullResult pullResult) {
        PullResultExt pullResultExt = (PullResultExt) pullResult;

        this.pullAPIWrapper.updatePullFromWhichNode(mq, pullResultExt.isSuggestPullingFromSlave());
        if (PullStatus.FOUND == pullResult.getPullStatus()) {
            ByteBuffer byteBuffer = ByteBuffer.wrap(pullResultExt.getMessageBinary());
            List<MessageExt> msgList = MessageDecoder.decodes(byteBuffer);
            pullResultExt.setMsgFoundList(msgList);
        }

        // 令GC释放内存
        pullResultExt.setMessageBinary(null);

        return pullResult;
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
                        pullCallback.onSuccess(DefaultMQPullConsumerImpl.this.processPullResult(mq, pullResult));
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


    public void updateConsumeOffset(MessageQueue mq, long offset) {

    }


    /**
     * 更新Consumer Offset，在Master断网期间，可能会更新到Slave，这里需要优化，或者在Slave端优化， TODO
     */
    public void updateConsumeOffsetToBroker(MessageQueue mq, long offset) throws RemotingException,
            MQBrokerException, InterruptedException, MQClientException {
        FindBrokerResult findBrokerResult = this.mQClientFactory.findBrokerAddressInAdmin(mq.getBrokerName());
        if (null == findBrokerResult) {
            // TODO 此处可能对Name Server压力过大，需要调优
            this.mQClientFactory.updateTopicRouteInfoFromNameServer(mq.getTopic());
            findBrokerResult = this.mQClientFactory.findBrokerAddressInAdmin(mq.getBrokerName());
        }

        if (findBrokerResult != null) {
            UpdateConsumerOffsetRequestHeader requestHeader = new UpdateConsumerOffsetRequestHeader();
            requestHeader.setTopic(mq.getTopic());
            requestHeader.setConsumerGroup(this.defaultMQPullConsumer.getConsumerGroup());
            requestHeader.setQueueId(mq.getQueueId());
            requestHeader.setCommitOffset(offset);

            this.mQClientFactory.getMQClientAPIImpl().updateConsumerOffset(findBrokerResult.getBrokerAddr(),
                requestHeader, 1000 * 5);
        }
        else {
            throw new MQClientException("The broker[" + mq.getBrokerName() + "] not exist", null);
        }
    }


    public long fetchConsumeOffsetFromBroker(MessageQueue mq) throws RemotingException, MQBrokerException,
            InterruptedException, MQClientException {
        FindBrokerResult findBrokerResult = this.mQClientFactory.findBrokerAddressInAdmin(mq.getBrokerName());
        if (null == findBrokerResult) {
            // TODO 此处可能对Name Server压力过大，需要调优
            this.mQClientFactory.updateTopicRouteInfoFromNameServer(mq.getTopic());
            findBrokerResult = this.mQClientFactory.findBrokerAddressInAdmin(mq.getBrokerName());
        }

        if (findBrokerResult != null) {
            QueryConsumerOffsetRequestHeader requestHeader = new QueryConsumerOffsetRequestHeader();
            requestHeader.setTopic(mq.getTopic());
            requestHeader.setConsumerGroup(this.defaultMQPullConsumer.getConsumerGroup());
            requestHeader.setQueueId(mq.getQueueId());

            return this.mQClientFactory.getMQClientAPIImpl().queryConsumerOffset(findBrokerResult.getBrokerAddr(),
                requestHeader, 1000 * 5);
        }
        else {
            throw new MQClientException("The broker[" + mq.getBrokerName() + "] not exist", null);
        }
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
    public void uploadConsumerOffsetsToBroker() {
        for (MessageQueue mq : this.offsetTable.keySet()) {
            AtomicLong offset = this.offsetTable.get(mq);
            if (offset != null) {
                try {
                    this.updateConsumeOffsetToBroker(mq, offset.get());
                    // TODO log
                }
                catch (Exception e) {
                    // TODO log
                }
            }
        }
    }
}
