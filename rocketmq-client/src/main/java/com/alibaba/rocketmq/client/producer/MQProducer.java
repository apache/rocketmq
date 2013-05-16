/**
 * $Id: MQProducer.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.client.producer;

import java.util.List;

import com.alibaba.rocketmq.client.MQAdmin;
import com.alibaba.rocketmq.client.exception.MQBrokerException;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.Message;
import com.alibaba.rocketmq.common.MessageQueue;
import com.alibaba.rocketmq.remoting.exception.RemotingException;


/**
 * 消息生产者
 * 
 * @author vintage.wang@gmail.com shijia.wxr@taobao.com
 */
public interface MQProducer extends MQAdmin {
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
     * 根据topic获取对应的MessageQueue，如果是顺序消息，则按照顺序消息配置返回
     * 
     * @param topic
     *            消息Topic
     * @return 返回队列集合
     * @throws MQClientException
     */
    public List<MessageQueue> fetchPublishMessageQueues(final String topic) throws MQClientException;


    /**
     * 发送消息，同步调用
     * 
     * @param msg
     *            消息
     * @return 发送结果
     * @throws InterruptedException
     * @throws MQBrokerException
     * @throws RemotingException
     * @throws MQClientException
     */
    public SendResult send(final Message msg) throws MQClientException, RemotingException, MQBrokerException,
            InterruptedException;


    /**
     * 发送消息，异步调用
     * 
     * @param msg
     *            消息
     * @param sendCallback
     *            发送结果通过此接口回调
     * @throws MQClientException
     * @throws RemotingException
     * @throws InterruptedException
     */
    public void send(final Message msg, final SendCallback sendCallback) throws MQClientException,
            RemotingException, InterruptedException;


    /**
     * 发送消息，Oneway形式，服务器不应答，无法保证消息是否成功到达服务器
     * 
     * @param msg
     *            消息
     * @throws MQClientException
     * @throws RemotingException
     * @throws InterruptedException
     */
    public void sendOneway(final Message msg) throws MQClientException, RemotingException, InterruptedException;


    /**
     * 向指定队列发送消息，同步调用
     * 
     * @param msg
     *            消息
     * @param mq
     *            队列
     * @return 发送结果
     * @throws InterruptedException
     * @throws MQBrokerException
     * @throws RemotingException
     * @throws MQClientException
     */
    public SendResult send(final Message msg, final MessageQueue mq) throws MQClientException, RemotingException,
            MQBrokerException, InterruptedException;


    /**
     * 向指定队列发送消息，异步调用
     * 
     * @param msg
     *            消息
     * @param mq
     *            队列
     * @param sendCallback
     *            发送结果通过此接口回调
     * @throws InterruptedException
     * @throws RemotingException
     * @throws MQClientException
     */
    public void send(final Message msg, final MessageQueue mq, final SendCallback sendCallback)
            throws MQClientException, RemotingException, InterruptedException;


    /**
     * 向指定队列发送消息，Oneway形式，服务器不应答，无法保证消息是否成功到达服务器
     * 
     * @param msg
     *            消息
     * @param mq
     *            队列
     * @throws MQClientException
     * @throws RemotingException
     * @throws InterruptedException
     */
    public void sendOneway(final Message msg, final MessageQueue mq) throws MQClientException, RemotingException,
            InterruptedException;


    /**
     * 发送消息，可以自定义选择队列，队列的总数可能会由于Broker的启停变化<br>
     * 如果要保证消息严格有序，在向Metaq运维人员申请Topic时，需要特别说明<br>
     * 同步调用
     * 
     * @param msg
     *            消息
     * @param selector
     *            队列选择器，发送时会回调
     * @param arg
     *            回调队列选择器时，此参数会传入队列选择方法
     * @return 发送结果
     * @throws InterruptedException
     * @throws MQBrokerException
     * @throws RemotingException
     * @throws MQClientException
     */
    public SendResult send(final Message msg, final MessageQueueSelector selector, final Object arg)
            throws MQClientException, RemotingException, MQBrokerException, InterruptedException;


    /**
     * 发送消息，可以自定义选择队列，队列的总数可能会由于Broker的启停变化<br>
     * 如果要保证消息严格有序，在向Metaq运维人员申请Topic时，需要特别说明<br>
     * 异步调用
     * 
     * @param msg
     *            消息
     * @param selector
     *            队列选择器，发送时会回调
     * @param arg
     *            回调队列选择器时，此参数会传入队列选择方法
     * @param sendCallback
     *            发送结果通过此接口回调
     * @throws MQClientException
     * @throws RemotingException
     * @throws InterruptedException
     */
    public void send(final Message msg, final MessageQueueSelector selector, final Object arg,
            final SendCallback sendCallback) throws MQClientException, RemotingException, InterruptedException;


    /**
     * 发送消息，可以自定义选择队列，队列的总数可能会由于Broker的启停变化<br>
     * 如果要保证消息严格有序，在向Metaq运维人员申请Topic时，需要特别说明<br>
     * Oneway形式，服务器不应答，无法保证消息是否成功到达服务器
     * 
     * @param msg
     *            消息
     * @param selector
     *            队列选择器，发送时会回调
     * @param arg
     *            回调队列选择器时，此参数会传入队列选择方法
     * @throws MQClientException
     * @throws RemotingException
     * @throws InterruptedException
     */
    public void sendOneway(final Message msg, final MessageQueueSelector selector, final Object arg)
            throws MQClientException, RemotingException, InterruptedException;


    public void sendMessageInTransaction(final Message msg, final LocalTransactionExecuter tranExecuter)
            throws MQClientException;
}
