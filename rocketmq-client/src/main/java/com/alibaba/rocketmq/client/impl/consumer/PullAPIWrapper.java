/**
 * $Id: PullAPIWrapper.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.client.impl.consumer;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import com.alibaba.rocketmq.client.consumer.PullCallback;
import com.alibaba.rocketmq.client.consumer.ConsumeFromWhichNode;
import com.alibaba.rocketmq.client.consumer.PullResult;
import com.alibaba.rocketmq.client.exception.MQBrokerException;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.impl.CommunicationMode;
import com.alibaba.rocketmq.client.impl.FindBrokerResult;
import com.alibaba.rocketmq.client.impl.factory.MQClientFactory;
import com.alibaba.rocketmq.common.MessageQueue;
import com.alibaba.rocketmq.common.protocol.header.PullMessageRequestHeader;
import com.alibaba.rocketmq.common.sysflag.PullSysFlag;
import com.alibaba.rocketmq.remoting.exception.RemotingException;


/**
 * 对Pull接口进行进一步的封装
 * 
 * @author vintage.wang@gmail.com shijia.wxr@taobao.com
 * 
 */
public class PullAPIWrapper {
    private ConcurrentHashMap<MessageQueue, AtomicBoolean/* suggestPullingFromSlave */> pullFromWhichNodeTable =
            new ConcurrentHashMap<MessageQueue, AtomicBoolean>(32);

    private final MQClientFactory mQClientFactory;
    private final String consumerGroup;
    private final ConsumeFromWhichNode consumeFromWhichNode;


    public PullAPIWrapper(MQClientFactory mQClientFactory, String consumerGroup,
            final ConsumeFromWhichNode pullFromWhere) {
        this.mQClientFactory = mQClientFactory;
        this.consumerGroup = consumerGroup;
        this.consumeFromWhichNode = pullFromWhere;
    }


    public void updatePullFromWhichNode(final MessageQueue mq, final boolean suggestPullingFromSlave) {
        AtomicBoolean suggest = this.pullFromWhichNodeTable.get(mq);
        if (null == suggest) {
            this.pullFromWhichNodeTable.put(mq, new AtomicBoolean(suggestPullingFromSlave));
        }
        else {
            suggest.set(suggestPullingFromSlave);
        }
    }


    /**
     * 每个队列都应该有相应的变量来保存从哪个服务器拉
     */
    public ConsumeFromWhichNode recalculatePullFromWhichNode(final MessageQueue mq) {
        switch (this.consumeFromWhichNode) {
        case CONSUME_FROM_MASTER_FIRST:
            AtomicBoolean suggest = this.pullFromWhichNodeTable.get(mq);
            if (suggest != null) {
                if (suggest.get()) {
                    return ConsumeFromWhichNode.CONSUME_FROM_SLAVE_FIRST;
                }
            }
            return ConsumeFromWhichNode.CONSUME_FROM_MASTER_FIRST;

        default:
            break;
        }
        return this.consumeFromWhichNode;
    }


    public PullResult pullKernelImpl(//
            final MessageQueue mq,// 1
            final String subExpression,// 2
            final long offset,// 3
            final int maxNums,// 4
            final int sysFlag,// 5
            final long commitOffset,// 6
            final long brokerSuspendMaxTimeMillis,// 7
            final long timeoutMillis,// 8
            final CommunicationMode communicationMode,// 9
            final PullCallback pullCallback// 10
    ) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        FindBrokerResult findBrokerResult =
                this.mQClientFactory.findBrokerAddressInSubscribe(mq.getBrokerName(),
                    this.recalculatePullFromWhichNode(mq));
        if (null == findBrokerResult) {
            // TODO 此处可能对Name Server压力过大，需要调优
            this.mQClientFactory.updateTopicRouteInfoFromNameServer(mq.getTopic());
            findBrokerResult =
                    this.mQClientFactory.findBrokerAddressInSubscribe(mq.getBrokerName(),
                        this.recalculatePullFromWhichNode(mq));
        }

        if (findBrokerResult != null) {
            int sysFlagInner = sysFlag;

            // Slave不允许实时提交消费进度，可以定时提交
            if (findBrokerResult.isSlave()) {
                sysFlagInner = PullSysFlag.clearCommitOffsetFlag(sysFlagInner);
            }

            PullMessageRequestHeader requestHeader = new PullMessageRequestHeader();
            requestHeader.setConsumerGroup(this.consumerGroup);
            requestHeader.setTopic(mq.getTopic());
            requestHeader.setQueueId(mq.getQueueId());
            requestHeader.setQueueOffset(offset);
            requestHeader.setMaxMsgNums(maxNums);
            requestHeader.setSysFlag(sysFlagInner);
            requestHeader.setCommitOffset(commitOffset);
            requestHeader.setSuspendTimeoutMillis(brokerSuspendMaxTimeMillis);
            requestHeader.setSubscription(subExpression);

            PullResult pullResult = this.mQClientFactory.getMQClientAPIImpl().pullMessage(//
                findBrokerResult.getBrokerAddr(),//
                requestHeader,//
                timeoutMillis,//
                communicationMode,//
                pullCallback);

            return pullResult;
        }

        throw new MQClientException("The broker[" + mq.getBrokerName() + "] not exist", null);
    }
}
