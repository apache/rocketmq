/**
 * $Id: MQConsumer.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.client.consumer;

import java.util.Set;

import com.alibaba.rocketmq.client.MQAdmin;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.message.MessageQueue;


/**
 * @author shijia.wxr<vintage.wang@gmail.com>
 * 
 */
public interface MQConsumer extends MQAdmin {
    /**
     * Consumer消费失败的消息可以选择重新发回到服务器端，并延时消费<br>
     * 会首先尝试将消息发回到消息之前存储的主机，此时只传送消息Offset，消息体不传送，不会占用网络带宽<br>
     * 如果发送失败，会自动重试发往其他主机，此时消息体也会传送<br>
     * 重传回去的消息只会被当前Consumer Group消费。
     * 
     * @param msg
     * @param mq
     * @param delayLevel
     */
    public void sendMessageBack(final MessageExt msg, final MessageQueue mq, final int delayLevel);


    /**
     * 根据topic获取对应的MessageQueue，是可被订阅的队列
     * 
     * @param topic
     *            消息Topic
     * @return 返回队列集合
     * @throws MQClientException
     */
    public Set<MessageQueue> fetchSubscribeMessageQueues(final String topic) throws MQClientException;

}
