/**
 * $Id: MQPullConsumer.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.client.consumer;

import java.util.List;

import com.alibaba.rocketmq.client.exception.MQBrokerException;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.MessageQueue;
import com.alibaba.rocketmq.remoting.exception.RemotingException;


/**
 * 消费者，主动方式消费
 * 
 * @author vintage.wang@gmail.com shijia.wxr@taobao.com
 */
public interface MQPullConsumer extends MQConsumer {
    /**
     * 启动服务
     * 
     * @throws MQClientException
     */
    public void start() throws MQClientException;


    /**
     * 关闭服务
     */
    public void shutdown();


    /**
     * 注册监听队列变化的listener对象
     * 
     * @param topic
     * @param listener
     *            一旦发生变化，客户端会主动回调listener对象
     */
    public void registerMessageQueueListener(final String topic, final MessageQueueListener listener);


    /**
     * 指定队列，主动拉取消息，即使没有消息，也立刻返回
     * 
     * @param mq
     *            指定具体要拉取的队列
     * @param subExpression
     *            订阅过滤表达式字符串，broker依据此表达式进行过滤。<br>
     *            eg: "tag1 || tag2 || tag3"<br>
     *            "tag1 || (tag2 && tag3)"<br>
     *            如果subExpression=null, 则表示全部订阅
     * @param offset
     *            从指定队列哪个位置开始拉取
     * @param maxNums
     *            一次最多拉取条数
     * @return 参见PullResult
     * @throws MQClientException
     * @throws InterruptedException
     * @throws MQBrokerException
     * @throws RemotingException
     */
    public PullResult pull(final MessageQueue mq, final String subExpression, final long offset, final int maxNums)
            throws MQClientException, RemotingException, MQBrokerException, InterruptedException;


    public void pull(final MessageQueue mq, final String subExpression, final long offset, final int maxNums,
            final PullCallback pullCallback) throws MQClientException, RemotingException, InterruptedException;


    /**
     * 指定队列，主动拉取消息，如果没有消息，则broker阻塞一段时间再返回（时间可配置）<br>
     * broker阻塞期间，如果有消息，则立刻将消息返回
     * 
     * @param mq
     *            指定具体要拉取的队列
     * @param subExpression
     *            订阅过滤表达式字符串，broker依据此表达式进行过滤。<br>
     *            eg: "tag1 || tag2 || tag3"<br>
     *            "tag1 || (tag2 && tag3)"<br>
     *            如果subExpression=null, 则表示全部订阅
     * @param offset
     *            从指定队列哪个位置开始拉取
     * @param maxNums
     *            一次最多拉取条数
     * @return 参见PullResult
     * @throws InterruptedException
     * @throws MQBrokerException
     * @throws RemotingException
     * @throws MQClientException
     */
    public PullResult pullBlockIfNotFound(final MessageQueue mq, final String subExpression, final long offset,
            final int maxNums) throws MQClientException, RemotingException, MQBrokerException,
            InterruptedException;


    public void pullBlockIfNotFound(final MessageQueue mq, final String subExpression, final long offset,
            final int maxNums, final PullCallback pullCallback) throws MQClientException, RemotingException,
            InterruptedException;


    public void updateConsumeOffset(final MessageQueue mq, final long offset) throws RemotingException,
            MQBrokerException, InterruptedException, MQClientException;


    public long fetchConsumeOffset(final MessageQueue mq) throws RemotingException, MQBrokerException,
            InterruptedException, MQClientException;


    /**
     * 根据topic获取MessageQueue，以均衡方式在组内多个成员之间分配
     * 
     * @param topic
     *            消息Topic
     * @return 成功 返回队列集合 失败 返回null
     * 
     */
    public List<MessageQueue> fetchMessageQueuesInBalance(final String topic);
}
