/**
 * $Id: DefaultMQPushConsumerImpl.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.client.impl.consumer;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.alibaba.rocketmq.client.QueryResult;
import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.MQPushConsumer;
import com.alibaba.rocketmq.client.consumer.PullCallback;
import com.alibaba.rocketmq.client.consumer.PullResult;
import com.alibaba.rocketmq.client.consumer.listener.MessageListener;
import com.alibaba.rocketmq.client.consumer.store.OffsetStore;
import com.alibaba.rocketmq.client.exception.MQBrokerException;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.impl.CommunicationMode;
import com.alibaba.rocketmq.client.impl.MQClientManager;
import com.alibaba.rocketmq.client.impl.factory.MQClientFactory;
import com.alibaba.rocketmq.common.MessageExt;
import com.alibaba.rocketmq.common.MessageQueue;
import com.alibaba.rocketmq.common.ServiceState;
import com.alibaba.rocketmq.common.TopicFilterType;
import com.alibaba.rocketmq.common.protocol.heartbeat.ConsumeType;
import com.alibaba.rocketmq.common.protocol.heartbeat.MessageModel;
import com.alibaba.rocketmq.common.protocol.heartbeat.SubscriptionData;
import com.alibaba.rocketmq.common.sysflag.PullSysFlag;
import com.alibaba.rocketmq.remoting.exception.RemotingException;


/**
 * @author shijia.wxr<vintage.wang@gmail.com>
 */
public class DefaultMQPushConsumerImpl implements MQPushConsumer, MQConsumerInner {
    private final DefaultMQPushConsumer defaultMQPushConsumer;
    private ServiceState serviceState = ServiceState.CREATE_JUST;
    private MQClientFactory mQClientFactory;
    private PullAPIWrapper pullAPIWrapper;

    // 消费进度存储
    private OffsetStore offsetStore;

    /**
     * 消息存储相关
     */
    private ConcurrentHashMap<MessageQueue, ProcessQueue> processQueueTable =
            new ConcurrentHashMap<MessageQueue, ProcessQueue>(64);


    public DefaultMQPushConsumerImpl(DefaultMQPushConsumer defaultMQPushConsumer) {
        this.defaultMQPushConsumer = defaultMQPushConsumer;
    }


    @Override
    public void sendMessageBack(MessageExt msg, MessageQueue mq, int delayLevel) {
        // TODO Auto-generated method stub
    }


    @Override
    public List<MessageQueue> fetchSubscribeMessageQueues(String topic) throws MQClientException {
        // TODO Auto-generated method stub
        return null;
    }


    @Override
    public void createTopic(String key, String newTopic, int queueNum, TopicFilterType topicFilterType,
            boolean order) throws MQClientException {
        // TODO Auto-generated method stub

    }


    @Override
    public long searchOffset(MessageQueue mq, long timestamp) throws MQClientException {
        // TODO Auto-generated method stub
        return 0;
    }


    @Override
    public long getMaxOffset(MessageQueue mq) throws MQClientException {
        // TODO Auto-generated method stub
        return 0;
    }


    @Override
    public long getMinOffset(MessageQueue mq) throws MQClientException {
        // TODO Auto-generated method stub
        return 0;
    }


    @Override
    public long getEarliestMsgStoreTime(MessageQueue mq) throws MQClientException {
        // TODO Auto-generated method stub
        return 0;
    }


    @Override
    public MessageExt viewMessage(String msgId) throws RemotingException, MQBrokerException, InterruptedException,
            MQClientException {
        // TODO Auto-generated method stub
        return null;
    }


    @Override
    public QueryResult queryMessage(String topic, String key, int maxNum, long begin, long end)
            throws MQClientException, InterruptedException {
        // TODO Auto-generated method stub
        return null;
    }


    @Override
    public void start() throws MQClientException {
        switch (this.serviceState) {
        case CREATE_JUST:
            this.serviceState = ServiceState.RUNNING;

            this.mQClientFactory =
                    MQClientManager.getInstance().getAndCreateMQClientFactory(
                        this.defaultMQPushConsumer.getmQClientConfig());

            this.pullAPIWrapper = new PullAPIWrapper(//
                mQClientFactory,//
                this.defaultMQPushConsumer.getConsumerGroup(),//
                this.defaultMQPushConsumer.getConsumeFromWhichNode());

            boolean registerOK =
                    mQClientFactory.registerConsumer(this.defaultMQPushConsumer.getConsumerGroup(), this);
            if (!registerOK) {
                this.serviceState = ServiceState.CREATE_JUST;
                throw new MQClientException("The consumer group[" + this.defaultMQPushConsumer.getConsumerGroup()
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


    @Override
    public void shutdown() {
        // TODO Auto-generated method stub

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
        // TODO Auto-generated method stub

    }


    @Override
    public void resume() {
        // TODO Auto-generated method stub
    }


    @Override
    public String getGroupName() {
        return null;
    }


    @Override
    public MessageModel getMessageModel() {
        return null;
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
     * 单线程调用
     */
    private ProcessQueue getAndCreateProcessQueue(final MessageQueue mq) {
        ProcessQueue pq = this.processQueueTable.get(mq);
        if (null == pq) {
            pq = new ProcessQueue();
            this.processQueueTable.put(mq, pq);
        }

        return pq;
    }


    public void pullMessage(final PullRequest pullRequest) {
        int sysFlag = PullSysFlag.buildSysFlag(//
            false, // commitOffset
            true, // suspend
            false// subscription
            );

        PullCallback pullCallback = new PullCallback() {

            @Override
            public void onSuccess(PullResult pullResult) {
            }


            @Override
            public void onException(Throwable e) {
            }
        };

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
        catch (MQClientException e) {
            e.printStackTrace();
        }
        catch (RemotingException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        catch (MQBrokerException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }


    @Override
    public void uploadConsumerOffsetsToBroker() {
        // TODO Auto-generated method stub

    }
}
