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
package com.alibaba.rocketmq.client;

import com.alibaba.rocketmq.client.exception.MQBrokerException;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.message.MessageQueue;
import com.alibaba.rocketmq.remoting.exception.RemotingException;


/**
 * MQ管理类接口
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-7-24
 */
public interface MQAdmin {
    /**
     * 创建topic
     * 
     * @param key
     *            请向运维人员申请
     * @param newTopic
     *            要创建的新topic
     * @param queueNum
     *            新topic队列数
     * @throws MQClientException
     */
    public void createTopic(final String key, final String newTopic, final int queueNum)
            throws MQClientException;


    /**
     * 根据时间查询对应的offset，精确到毫秒<br>
     * P.S. 当前接口有较多IO开销，请勿频繁调用
     * 
     * @param mq
     *            队列
     * @param timestamp
     *            毫秒形式时间戳
     * @return 指定时间对应的offset
     * @throws MQClientException
     */
    public long searchOffset(final MessageQueue mq, final long timestamp) throws MQClientException;


    /**
     * 向服务器查询队列最大Offset PS: 最大Offset无对应消息，减1有消息
     * 
     * @param mq
     *            队列
     * @return 队列的最大Offset
     * @throws MQClientException
     */
    public long maxOffset(final MessageQueue mq) throws MQClientException;


    /**
     * 向服务器查询队列最小Offset PS: 最小Offset有对应消息
     * 
     * @param mq
     *            队列
     * @return 队列的最小Offset
     * @throws MQClientException
     */
    public long minOffset(final MessageQueue mq) throws MQClientException;


    /**
     * 向服务器查询队列保存的最早消息对应的存储时间
     * 
     * @param mq
     *            队列
     * @return 最早消息对应的存储时间，精确到毫秒
     * @throws MQClientException
     */
    public long earliestMsgStoreTime(final MessageQueue mq) throws MQClientException;


    /**
     * 根据消息ID，从服务器获取完整的消息
     * 
     * @param msgId
     * @return 完整消息
     * @throws InterruptedException
     * @throws MQBrokerException
     * @throws RemotingException
     * @throws MQClientException
     */
    public MessageExt viewMessage(final String msgId) throws RemotingException, MQBrokerException,
            InterruptedException, MQClientException;


    /**
     * 根据消息Key查询消息
     * 
     * @param topic
     *            消息主题
     * @param key
     *            消息关键词
     * @param maxNum
     *            查询最大条数
     * @param begin
     *            起始时间戳
     * @param end
     *            结束时间戳
     * @return 查询结果
     * @throws MQClientException
     * @throws InterruptedException
     */
    public QueryResult queryMessage(final String topic, final String key, final int maxNum, final long begin,
            final long end) throws MQClientException, InterruptedException;
}
