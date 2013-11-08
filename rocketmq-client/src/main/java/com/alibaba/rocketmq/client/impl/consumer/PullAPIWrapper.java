/**
 * Copyright (C) 2010-2013 Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.rocketmq.client.impl.consumer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import com.alibaba.rocketmq.client.VirtualEnvUtil;
import com.alibaba.rocketmq.client.consumer.PullCallback;
import com.alibaba.rocketmq.client.consumer.PullResult;
import com.alibaba.rocketmq.client.consumer.PullStatus;
import com.alibaba.rocketmq.client.exception.MQBrokerException;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.impl.CommunicationMode;
import com.alibaba.rocketmq.client.impl.FindBrokerResult;
import com.alibaba.rocketmq.client.impl.factory.MQClientFactory;
import com.alibaba.rocketmq.common.MixAll;
import com.alibaba.rocketmq.common.UtilAll;
import com.alibaba.rocketmq.common.message.MessageConst;
import com.alibaba.rocketmq.common.message.MessageDecoder;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.message.MessageQueue;
import com.alibaba.rocketmq.common.protocol.header.PullMessageRequestHeader;
import com.alibaba.rocketmq.common.protocol.heartbeat.SubscriptionData;
import com.alibaba.rocketmq.common.sysflag.PullSysFlag;
import com.alibaba.rocketmq.remoting.exception.RemotingException;


/**
 * 对Pull接口进行进一步的封装
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-7-24
 */
public class PullAPIWrapper {
    private ConcurrentHashMap<MessageQueue, AtomicLong/* brokerId */> pullFromWhichNodeTable =
            new ConcurrentHashMap<MessageQueue, AtomicLong>(32);

    private final MQClientFactory mQClientFactory;
    private final String consumerGroup;


    public PullAPIWrapper(MQClientFactory mQClientFactory, String consumerGroup) {
        this.mQClientFactory = mQClientFactory;
        this.consumerGroup = consumerGroup;
    }


    public void updatePullFromWhichNode(final MessageQueue mq, final long brokerId) {
        AtomicLong suggest = this.pullFromWhichNodeTable.get(mq);
        if (null == suggest) {
            this.pullFromWhichNodeTable.put(mq, new AtomicLong(brokerId));
        }
        else {
            suggest.set(brokerId);
        }
    }


    /**
     * 对拉取结果进行处理，主要是消息反序列化
     * 
     * @param mq
     * @param pullResult
     * @param subscriptionData
     * @param projectGroupPrefix
     *            虚拟环境projectGroupPrefix，不存在可设置为 null
     * @return
     */
    public PullResult processPullResult(final MessageQueue mq, final PullResult pullResult,
            final SubscriptionData subscriptionData) {
        final String projectGroupPrefix = this.mQClientFactory.getMQClientAPIImpl().getProjectGroupPrefix();
        PullResultExt pullResultExt = (PullResultExt) pullResult;

        this.updatePullFromWhichNode(mq, pullResultExt.getSuggestWhichBrokerId());
        if (PullStatus.FOUND == pullResult.getPullStatus()) {
            ByteBuffer byteBuffer = ByteBuffer.wrap(pullResultExt.getMessageBinary());
            List<MessageExt> msgList = MessageDecoder.decodes(byteBuffer);

            // 消息再次过滤
            List<MessageExt> msgListFilterAgain = msgList;
            if (!subscriptionData.getTagsSet().isEmpty()) {
                msgListFilterAgain = new ArrayList<MessageExt>(msgList.size());
                for (MessageExt msg : msgList) {
                    if (msg.getTags() != null) {
                        if (subscriptionData.getTagsSet().contains(msg.getTags())) {
                            msgListFilterAgain.add(msg);
                        }
                    }
                }
            }

            // 清除虚拟运行环境相关的projectGroupPrefix
            if (!UtilAll.isBlank(projectGroupPrefix)) {
                subscriptionData.setTopic(VirtualEnvUtil.clearProjectGroup(subscriptionData.getTopic(),
                    projectGroupPrefix));
                mq.setTopic(VirtualEnvUtil.clearProjectGroup(mq.getTopic(), projectGroupPrefix));
                for (MessageExt msg : msgListFilterAgain) {
                    msg.setTopic(VirtualEnvUtil.clearProjectGroup(msg.getTopic(), projectGroupPrefix));
                    // 消息中放入队列的最大最小Offset，方便应用来感知消息堆积程度
                    msg.putProperty(MessageConst.PROPERTY_MIN_OFFSET,
                        Long.toString(pullResult.getMinOffset()));
                    msg.putProperty(MessageConst.PROPERTY_MAX_OFFSET,
                        Long.toString(pullResult.getMaxOffset()));
                }
            }
            else {
                // 消息中放入队列的最大最小Offset，方便应用来感知消息堆积程度
                for (MessageExt msg : msgListFilterAgain) {
                    msg.putProperty(MessageConst.PROPERTY_MIN_OFFSET,
                        Long.toString(pullResult.getMinOffset()));
                    msg.putProperty(MessageConst.PROPERTY_MAX_OFFSET,
                        Long.toString(pullResult.getMaxOffset()));
                }
            }

            pullResultExt.setMsgFoundList(msgListFilterAgain);
        }

        // 令GC释放内存
        pullResultExt.setMessageBinary(null);

        return pullResult;
    }


    /**
     * 每个队列都应该有相应的变量来保存从哪个服务器拉
     */
    public long recalculatePullFromWhichNode(final MessageQueue mq) {
        AtomicLong suggest = this.pullFromWhichNodeTable.get(mq);
        if (suggest != null) {
            return suggest.get();
        }

        return MixAll.MASTER_ID;
    }


    public PullResult pullKernelImpl(//
            final MessageQueue mq,// 1
            final String subExpression,// 2
            final long subVersion,// 3
            final long offset,// 4
            final int maxNums,// 5
            final int sysFlag,// 6
            final long commitOffset,// 7
            final long brokerSuspendMaxTimeMillis,// 8
            final long timeoutMillis,// 9
            final CommunicationMode communicationMode,// 10
            final PullCallback pullCallback// 11
    ) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        FindBrokerResult findBrokerResult =
                this.mQClientFactory.findBrokerAddressInSubscribe(mq.getBrokerName(),
                    this.recalculatePullFromWhichNode(mq), false);
        if (null == findBrokerResult) {
            // TODO 此处可能对Name Server压力过大，需要调优
            this.mQClientFactory.updateTopicRouteInfoFromNameServer(mq.getTopic());
            findBrokerResult =
                    this.mQClientFactory.findBrokerAddressInSubscribe(mq.getBrokerName(),
                        this.recalculatePullFromWhichNode(mq), false);
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
            requestHeader.setSubVersion(subVersion);

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
