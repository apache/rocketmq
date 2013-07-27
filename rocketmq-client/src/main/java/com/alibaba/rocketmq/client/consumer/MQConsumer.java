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
package com.alibaba.rocketmq.client.consumer;

import java.util.Set;

import com.alibaba.rocketmq.client.MQAdmin;
import com.alibaba.rocketmq.client.exception.MQBrokerException;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.message.MessageQueue;
import com.alibaba.rocketmq.remoting.exception.RemotingException;


/**
 * Consumer接口
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-7-24
 */
public interface MQConsumer extends MQAdmin {
    /**
     * Consumer消费失败的消息可以选择重新发回到服务器端，并延时消费<br>
     * 会首先尝试将消息发回到消息之前存储的主机，此时只传送消息Offset，消息体不传送，不会占用网络带宽<br>
     * 如果发送失败，会自动重试发往其他主机，此时消息体也会传送<br>
     * 重传回去的消息只会被当前Consumer Group消费。
     * 
     * @param msg
     * @param delayLevel
     * @throws InterruptedException
     * @throws MQBrokerException
     * @throws RemotingException
     * @throws MQClientException
     */
    public void sendMessageBack(final MessageExt msg, final int delayLevel) throws RemotingException,
            MQBrokerException, InterruptedException, MQClientException;


    /**
     * 根据topic获取对应的MessageQueue，是可被订阅的队列<br>
     * P.S 从Consumer Cache中拿数据，可以频繁调用。Cache中数据大约30秒更新一次
     * 
     * @param topic
     *            消息Topic
     * @return 返回队列集合
     * @throws MQClientException
     */
    public Set<MessageQueue> fetchSubscribeMessageQueues(final String topic) throws MQClientException;
}
