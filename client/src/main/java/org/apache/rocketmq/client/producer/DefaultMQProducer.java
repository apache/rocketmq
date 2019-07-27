/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.client.producer;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutorService;
import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.QueryResult;
import org.apache.rocketmq.client.Validators;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.client.trace.AsyncTraceDispatcher;
import org.apache.rocketmq.client.trace.TraceDispatcher;
import org.apache.rocketmq.client.trace.hook.SendMessageTraceHookImpl;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageBatch;
import org.apache.rocketmq.common.message.MessageClientIDSetter;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageId;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.remoting.netty.NettyRemotingClient;

/**
 * This class is the entry point for applications intending to send messages.
 * </p>
 *
 * It's fine to tune fields which exposes getter/setter methods, but keep in mind, all of them should work well out of
 * box for most scenarios.
 * </p>
 *
 * This class aggregates various <code>send</code> methods to deliver messages to brokers. Each of them has pros and
 * cons; you'd better understand strengths and weakness of them before actually coding.
 * </p>
 *
 * <p>
 * <strong>Thread Safety:</strong> After configuring and starting process, this class can be regarded as thread-safe
 * and used among multiple threads context.
 * </p>
 *
 * 中文：
 * 此类是打算发送消息的应用程序的入口点。
 *
 * 可以对公开getter/setter方法的字段进行优化，但请记住，所有这些方法都应该在大多数场景的适应。
 *
 * 这个类聚合了各种发送方法来向代理传递消息。他们每个人都有优点和缺点：在实际编码之前，您最好了解它们的优点和缺点。
 *
 * 线程安全：在配置和启动进程之后，这个类可以被视为线程安全的。在多线程上下文中使用。
 *
 */
public class DefaultMQProducer extends ClientConfig implements MQProducer {

    private final InternalLogger log = ClientLogger.getLog();

    /**
     * Wrapping internal implementations for virtually all methods presented in this class.
     * 包装这个类中几乎所有方法的内部实现。
     */
    protected final transient DefaultMQProducerImpl defaultMQProducerImpl;

    /**
     * Producer group conceptually aggregates all producer instances of exactly same role, which is particularly
     * important when transactional messages are involved.
     * 生产者组在概念上聚合所有具有完全相同角色的生产者实例，这在涉及事务性消息时尤为重要。
     * </p>
     *
     * For non-transactional messages, it does not matter as long as it's unique per process.
     * 对于非事务性消息，只要每个进程都是唯一的，就不重要。
     * </p>
     *
     * See {@linktourl http://rocketmq.apache.org/docs/core-concept/} for more discussion.
     */
    private String producerGroup;

    /**
     * Just for testing or demo program
     * 仅用于测试或演示程序
     */
    private String createTopicKey = MixAll.AUTO_CREATE_TOPIC_KEY_TOPIC;

    /**
     * Number of queues to create per default topic.
     * 每个默认主题要创建的队列数。
     */
    private volatile int defaultTopicQueueNums = 4;

    /**
     * Timeout for sending messages.
     * 发送消息超时时间
     */
    private int sendMsgTimeout = 3000;

    /**
     * Compress message body threshold, namely, message body larger than 4k will be compressed on default.
     * 压缩消息体阈值，即默认压缩大于4K的消息体。
     */
    private int compressMsgBodyOverHowmuch = 1024 * 4;

    /**
     * Maximum number of retry to perform internally before claiming sending failure in synchronous mode.
     * 在同步模式下声明发送失败之前要在内部执行的最大重试次数。
     * </p>
     *
     * This may potentially cause message duplication which is up to application developers to resolve.
     * 这可能会导致消息重复，这由应用程序开发人员来解决。
     */
    private int retryTimesWhenSendFailed = 2;

    /**
     * Maximum number of retry to perform internally before claiming sending failure in asynchronous mode.
     * 在异步模式下声明发送失败之前要在内部执行的最大重试次数。
     * </p>
     *
     * This may potentially cause message duplication which is up to application developers to resolve.
     * 这可能会导致消息重复，这由应用程序开发人员来解决。
     */
    private int retryTimesWhenSendAsyncFailed = 2;

    /**
     * Indicate whether to retry another broker on sending failure internally.
     * 指示在内部发送失败时是否重试其他代理。
     */
    private boolean retryAnotherBrokerWhenNotStoreOK = false;

    /**
     * Maximum allowed message size in bytes.
     * 允许的最大消息大小（字节）
     */
    private int maxMessageSize = 1024 * 1024 * 4; // 4M

    /**
     * Interface of asynchronous transfer data
     * 异步传输数据接口（消息跟踪）
     */
    private TraceDispatcher traceDispatcher = null;

    /**
     * Default constructor.
     */
    public DefaultMQProducer() {
        this(MixAll.DEFAULT_PRODUCER_GROUP, null);
    }

    /**
     * Constructor specifying both producer group and RPC hook.
     * 同时指定生产者组和RPC挂钩的构造函数。
     *
     * @param producerGroup Producer group, see the name-sake field.
     * @param rpcHook RPC hook to execute per each remoting command execution.
     */
    public DefaultMQProducer(final String producerGroup, RPCHook rpcHook) {
        this.producerGroup = producerGroup;
        defaultMQProducerImpl = new DefaultMQProducerImpl(this, rpcHook);
    }

    /**
     * Constructor specifying producer group, RPC hook, enabled msgTrace flag and customized trace topic name.
     * 造函数，指定生产者组、rpc挂钩、启用的msgtrace标志和自定义跟踪主题名称。
     *
     * @param producerGroup Producer group, see the name-sake field.
     * @param rpcHook RPC hook to execute per each remoting command execution.
     * @param enableMsgTrace Switch flag instance for message trace.
     * @param customizedTraceTopic The name value of message trace topic.If you don't config,you can use the default trace topic name.
     */
    public DefaultMQProducer(final String producerGroup, RPCHook rpcHook, boolean enableMsgTrace,final String customizedTraceTopic) {
        this.producerGroup = producerGroup;
        defaultMQProducerImpl = new DefaultMQProducerImpl(this, rpcHook);
        //if client open the message trace feature
        if (enableMsgTrace) {
            try {
                AsyncTraceDispatcher dispatcher = new AsyncTraceDispatcher(customizedTraceTopic, rpcHook);
                dispatcher.setHostProducer(this.getDefaultMQProducerImpl());
                traceDispatcher = dispatcher;
                this.getDefaultMQProducerImpl().registerSendMessageHook(
                    new SendMessageTraceHookImpl(traceDispatcher));
            } catch (Throwable e) {
                log.error("system mqtrace hook init failed ,maybe can't send msg trace data");
            }
        }
    }

    /**
     * Constructor specifying producer group.
     * 指定生产商的构造函数
     *
     * @param producerGroup Producer group, see the name-sake field.
     */
    public DefaultMQProducer(final String producerGroup) {
        this(producerGroup, null);
    }

    /**
     * Constructor specifying producer group and enabled msg trace flag.
     * 指定生产者组和已启用的消息跟踪标志的构造函数。
     *
     * @param producerGroup Producer group, see the name-sake field.
     * @param enableMsgTrace Switch flag instance for message trace.
     */
    public DefaultMQProducer(final String producerGroup, boolean enableMsgTrace) {
        this(producerGroup, null, enableMsgTrace, null);
    }


    /**
     * Constructor specifying producer group, enabled msgTrace flag and customized trace topic name.
     *
     * @param producerGroup Producer group, see the name-sake field.
     * @param enableMsgTrace Switch flag instance for message trace.
     * @param customizedTraceTopic The name value of message trace topic.If you don't config,you can use the default trace topic name.
     */
    public DefaultMQProducer(final String producerGroup, boolean enableMsgTrace, final String customizedTraceTopic) {
        this(producerGroup, null, enableMsgTrace, customizedTraceTopic);
    }

    /**
     * Constructor specifying the RPC hook.
     * 
     * @param rpcHook RPC hook to execute per each remoting command execution.
     */
    public DefaultMQProducer(RPCHook rpcHook) {
        this(MixAll.DEFAULT_PRODUCER_GROUP, rpcHook);
    }

    /**
     * Start this producer instance.
     * 启动此生成器实例
     * </p>
     *
     * <strong>
     * Much internal initializing procedures are carried out to make this instance prepared, thus, it's a must to invoke
     * this method before sending or querying messages.
     * 为了准备这个实例，需要执行许多内部初始化过程，因此在发送或查询消息之前必须调用这个方法。
     * </strong>
     * </p>
     *
     * @throws MQClientException if there is any unexpected error.
     */
    @Override
    public void start() throws MQClientException {
        this.defaultMQProducerImpl.start();
        if (null != traceDispatcher) {
            try {
                traceDispatcher.start(this.getNamesrvAddr());
            } catch (MQClientException e) {
                log.warn("trace dispatcher start failed ", e);
            }
        }
    }

    /**
     * This method shuts down this producer instance and releases related resources.
     * 此方法关闭此生成器实例并释放相关资源。
     */
    @Override
    public void shutdown() {
        this.defaultMQProducerImpl.shutdown();
        if (null != traceDispatcher) {
            traceDispatcher.shutdown();
        }
    }

    /**
     * Fetch message queues of topic <code>topic</code>, to which we may send/publish messages.
     * 获取主题的消息队列，我们可以向其发送/发布消息
     *
     * @param topic Topic to fetch.
     * @return List of message queues readily to send messages to
     * @throws MQClientException if there is any client error.
     */
    @Override
    public List<MessageQueue> fetchPublishMessageQueues(String topic) throws MQClientException {
        return this.defaultMQProducerImpl.fetchPublishMessageQueues(topic);
    }

    /**
     * Send message in synchronous mode. This method returns only when the sending procedure totally completes.
     * 以同步模式发送消息。此方法仅在发送过程完全完成时返回。
     * </p>
     *
     * <strong>Warn:</strong> this method has internal retry-mechanism, that is, internal implementation will retry
     * {@link #retryTimesWhenSendFailed} times before claiming failure. As a result, multiple messages may potentially
     * delivered to broker(s). It's up to the application developers to resolve potential duplication issue.
     * 警告：此方法具有内部重试机制，即内部实现在声明失败之前将重试次。
     * 因此，可能会将多条消息传递给代理。由应用程序开发人员来解决潜在的重复问题。
     *
     * @param msg Message to send.
     * @return {@link SendResult} instance to inform senders details of the deliverable, say Message ID of the message,
     * {@link SendStatus} indicating broker storage/replication status, message queue sent to, etc.
     * @throws MQClientException if there is any client error.
     * @throws RemotingException if there is any network-tier error.
     * @throws MQBrokerException if there is any error with broker.
     * @throws InterruptedException if the sending thread is interrupted.
     */
    @Override
    public SendResult send(
        Message msg) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return this.defaultMQProducerImpl.send(msg);
    }

    /**
     * Same to {@link #send(Message)} with send timeout specified in addition.
     * 发送消息并指定超时时间
     *
     * @param msg Message to send.
     * @param timeout send timeout.
     * @return {@link SendResult} instance to inform senders details of the deliverable, say Message ID of the message,
     * {@link SendStatus} indicating broker storage/replication status, message queue sent to, etc.
     * @throws MQClientException if there is any client error.
     * @throws RemotingException if there is any network-tier error.
     * @throws MQBrokerException if there is any error with broker.
     * @throws InterruptedException if the sending thread is interrupted.
     */
    @Override
    public SendResult send(Message msg,
        long timeout) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return this.defaultMQProducerImpl.send(msg, timeout);
    }

    /**
     * Send message to broker asynchronously.
     * 异步发送消息给代理
     * </p>
     *
     * This method returns immediately. On sending completion, <code>sendCallback</code> will be executed.
     * 此方法立即返回。发送完成后，将执行sendCallback
     * </p>
     *
     * Similar to {@link #send(Message)}, internal implementation would potentially retry up to
     * {@link #retryTimesWhenSendAsyncFailed} times before claiming sending failure, which may yield message duplication
     * and application developers are the one to resolve this potential issue.
     * 与类似，内部实现可能会在声称发送失败之前重试多次，这可能导致消息重复，应用程序开发人员是解决此潜在问题的人员。
     *
     * @param msg Message to send.
     * @param sendCallback Callback to execute on sending completed, either successful or unsuccessful.
     * @throws MQClientException if there is any client error.
     * @throws RemotingException if there is any network-tier error.
     * @throws InterruptedException if the sending thread is interrupted.
     */
    @Override
    public void send(Message msg,
        SendCallback sendCallback) throws MQClientException, RemotingException, InterruptedException {
        this.defaultMQProducerImpl.send(msg, sendCallback);
    }

    /**
     * Same to {@link #send(Message, SendCallback)} with send timeout specified in addition.
     * 异步发送并指定超时时间
     * @param msg message to send.
     * @param sendCallback Callback to execute.
     * @param timeout send timeout.
     * @throws MQClientException if there is any client error.
     * @throws RemotingException if there is any network-tier error.
     * @throws InterruptedException if the sending thread is interrupted.
     */
    @Override
    public void send(Message msg, SendCallback sendCallback, long timeout)
        throws MQClientException, RemotingException, InterruptedException {
        this.defaultMQProducerImpl.send(msg, sendCallback, timeout);
    }

    /**
     * Similar to <a href="https://en.wikipedia.org/wiki/User_Datagram_Protocol">UDP</a>, this method won't wait for
     * acknowledgement from broker before return. Obviously, it has maximums throughput yet potentials of message loss.
     * 类似于这种方法，在返回之前不会等待代理的确认。显然，它具有最大的吞吐量，但也有可能丢失消息。
     *
     * @param msg Message to send.
     * @throws MQClientException if there is any client error.
     * @throws RemotingException if there is any network-tier error.
     * @throws InterruptedException if the sending thread is interrupted.
     */
    @Override
    public void sendOneway(Message msg) throws MQClientException, RemotingException, InterruptedException {
        this.defaultMQProducerImpl.sendOneway(msg);
    }

    /**
     * Same to {@link #send(Message)} with target message queue specified in addition.
     * 指定的消息队列发送消息。
     *
     * @param msg Message to send.
     * @param mq Target message queue.
     * @return {@link SendResult} instance to inform senders details of the deliverable, say Message ID of the message,
     * {@link SendStatus} indicating broker storage/replication status, message queue sent to, etc.
     * @throws MQClientException if there is any client error.
     * @throws RemotingException if there is any network-tier error.
     * @throws MQBrokerException if there is any error with broker.
     * @throws InterruptedException if the sending thread is interrupted.
     */
    @Override
    public SendResult send(Message msg, MessageQueue mq)
        throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return this.defaultMQProducerImpl.send(msg, mq);
    }

    /**
     * Same to {@link #send(Message)} with target message queue and send timeout specified.
     * 指定的消息队列发送消息，并指定超时时间
     *
     * @param msg Message to send.
     * @param mq Target message queue.
     * @param timeout send timeout.
     * @return {@link SendResult} instance to inform senders details of the deliverable, say Message ID of the message,
     * {@link SendStatus} indicating broker storage/replication status, message queue sent to, etc.
     * @throws MQClientException if there is any client error.
     * @throws RemotingException if there is any network-tier error.
     * @throws MQBrokerException if there is any error with broker.
     * @throws InterruptedException if the sending thread is interrupted.
     */
    @Override
    public SendResult send(Message msg, MessageQueue mq, long timeout)
        throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return this.defaultMQProducerImpl.send(msg, mq, timeout);
    }

    /**
     * Same to {@link #send(Message, SendCallback)} with target message queue specified.
     * 异步发送消息到指定的消息队列，此方法立即返回，执行成功后回调SendCallback
     * @param msg Message to send.
     * @param mq Target message queue.
     * @param sendCallback Callback to execute on sending completed, either successful or unsuccessful.
     * @throws MQClientException if there is any client error.
     * @throws RemotingException if there is any network-tier error.
     * @throws InterruptedException if the sending thread is interrupted.
     */
    @Override
    public void send(Message msg, MessageQueue mq, SendCallback sendCallback)
        throws MQClientException, RemotingException, InterruptedException {
        this.defaultMQProducerImpl.send(msg, mq, sendCallback);
    }

    /**
     * Same to {@link #send(Message, SendCallback)} with target message queue and send timeout specified.
     * 异步发送消息到指定的消息队列，此方法立即返回，执行成功后回调SendCallback，并指定超时时间
     * @param msg Message to send.
     * @param mq Target message queue.
     * @param sendCallback Callback to execute on sending completed, either successful or unsuccessful.
     * @param timeout Send timeout.
     * @throws MQClientException if there is any client error.
     * @throws RemotingException if there is any network-tier error.
     * @throws InterruptedException if the sending thread is interrupted.
     */
    @Override
    public void send(Message msg, MessageQueue mq, SendCallback sendCallback, long timeout)
        throws MQClientException, RemotingException, InterruptedException {
        this.defaultMQProducerImpl.send(msg, mq, sendCallback, timeout);
    }

    /**
     * Same to {@link #sendOneway(Message)} with target message queue specified.
     * 单向发送消息到指定的消息队列，不等代理响应返回，有消息丢失的概率
     * @param msg Message to send.
     * @param mq Target message queue.
     * @throws MQClientException if there is any client error.
     * @throws RemotingException if there is any network-tier error.
     * @throws InterruptedException if the sending thread is interrupted.
     */
    @Override
    public void sendOneway(Message msg,
        MessageQueue mq) throws MQClientException, RemotingException, InterruptedException {
        this.defaultMQProducerImpl.sendOneway(msg, mq);
    }

    /**
     * Same to {@link #send(Message)} with message queue selector specified.
     * 通过消息队列选择器选择要发送消息的消息队列
     * @param msg Message to send.
     * @param selector Message queue selector, through which we get target message queue to deliver message to.
     * @param arg Argument to work along with message queue selector.
     * @return {@link SendResult} instance to inform senders details of the deliverable, say Message ID of the message,
     * {@link SendStatus} indicating broker storage/replication status, message queue sent to, etc.
     * @throws MQClientException if there is any client error.
     * @throws RemotingException if there is any network-tier error.
     * @throws MQBrokerException if there is any error with broker.
     * @throws InterruptedException if the sending thread is interrupted.
     */
    @Override
    public SendResult send(Message msg, MessageQueueSelector selector, Object arg)
        throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return this.defaultMQProducerImpl.send(msg, selector, arg);
    }

    /**
     * Same to {@link #send(Message, MessageQueueSelector, Object)} with send timeout specified.
     * 通过消息队列选择器选择要发送消息的消息队列，并指定超时时间
     * @param msg Message to send.
     * @param selector Message queue selector, through which we get target message queue to deliver message to.
     * @param arg Argument to work along with message queue selector.
     * @param timeout Send timeout.
     * @return {@link SendResult} instance to inform senders details of the deliverable, say Message ID of the message,
     * {@link SendStatus} indicating broker storage/replication status, message queue sent to, etc.
     * @throws MQClientException if there is any client error.
     * @throws RemotingException if there is any network-tier error.
     * @throws MQBrokerException if there is any error with broker.
     * @throws InterruptedException if the sending thread is interrupted.
     */
    @Override
    public SendResult send(Message msg, MessageQueueSelector selector, Object arg, long timeout)
        throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return this.defaultMQProducerImpl.send(msg, selector, arg, timeout);
    }

    /**
     * Same to {@link #send(Message, SendCallback)} with message queue selector specified.
     * 通过消息队列选择器选择要发送消息的消息队列，进行异步发送，指定完回调SendCallback
     * @param msg Message to send.
     * @param selector Message selector through which to get target message queue.
     * @param arg Argument used along with message queue selector.
     * @param sendCallback callback to execute on sending completion.
     * @throws MQClientException if there is any client error.
     * @throws RemotingException if there is any network-tier error.
     * @throws InterruptedException if the sending thread is interrupted.
     */
    @Override
    public void send(Message msg, MessageQueueSelector selector, Object arg, SendCallback sendCallback)
        throws MQClientException, RemotingException, InterruptedException {
        this.defaultMQProducerImpl.send(msg, selector, arg, sendCallback);
    }

    /**
     * Same to {@link #send(Message, MessageQueueSelector, Object, SendCallback)} with timeout specified.
     * 通过消息队列选择器选择要发送消息的消息队列，进行异步发送，指定完回调SendCallback，并指定消息超时时间
     * @param msg Message to send.
     * @param selector Message selector through which to get target message queue.
     * @param arg Argument used along with message queue selector.
     * @param sendCallback callback to execute on sending completion.
     * @param timeout Send timeout.
     * @throws MQClientException if there is any client error.
     * @throws RemotingException if there is any network-tier error.
     * @throws InterruptedException if the sending thread is interrupted.
     */
    @Override
    public void send(Message msg, MessageQueueSelector selector, Object arg, SendCallback sendCallback, long timeout)
        throws MQClientException, RemotingException, InterruptedException {
        this.defaultMQProducerImpl.send(msg, selector, arg, sendCallback, timeout);
    }

    /**
     * Same to {@link #sendOneway(Message)} with message queue selector specified.
     * 通过消息队列选择器选择要发送消息的消息队列，单向发送
     * @param msg Message to send.
     * @param selector Message queue selector, through which to determine target message queue to deliver message
     * @param arg Argument used along with message queue selector.
     * @throws MQClientException if there is any client error.
     * @throws RemotingException if there is any network-tier error.
     * @throws InterruptedException if the sending thread is interrupted.
     */
    @Override
    public void sendOneway(Message msg, MessageQueueSelector selector, Object arg)
        throws MQClientException, RemotingException, InterruptedException {
        this.defaultMQProducerImpl.sendOneway(msg, selector, arg);
    }

    /**
     * This method is to send transactional messages.
     * 发送事物消息（不让用，直接丢出异常）
     * @param msg Transactional message to send.
     * @param tranExecuter local transaction executor.
     * @param arg Argument used along with local transaction executor.
     * @return Transaction result.
     * @throws MQClientException if there is any client error.
     */
    @Override
    public TransactionSendResult sendMessageInTransaction(Message msg, LocalTransactionExecuter tranExecuter,
        final Object arg)
        throws MQClientException {
        throw new RuntimeException("sendMessageInTransaction not implement, please use TransactionMQProducer class");
    }

    /**
     * This method is used to send transactional messages.
     * 发送事物消息（不让用，直接丢出异常）
     * @param msg Transactional message to send.
     * @param arg Argument used along with local transaction executor.
     * @return Transaction result.
     * @throws MQClientException
     */
    @Override
    public TransactionSendResult sendMessageInTransaction(Message msg,
        Object arg) throws MQClientException {
        throw new RuntimeException("sendMessageInTransaction not implement, please use TransactionMQProducer class");
    }

    /**
     * Create a topic on broker.
     * 在代理上创建一个主题
     * @param key accesskey
     * @param newTopic topic name
     * @param queueNum topic's queue number
     * @throws MQClientException if there is any client error.
     */
    @Override
    public void createTopic(String key, String newTopic, int queueNum) throws MQClientException {
        createTopic(key, newTopic, queueNum, 0);
    }

    /**
     * Create a topic on broker.
     * 在代理上创建一个主题，指定系统标识
     * @param key accesskey
     * @param newTopic topic name
     * @param queueNum topic's queue number
     * @param topicSysFlag topic system flag
     * @throws MQClientException if there is any client error.
     */
    @Override
    public void createTopic(String key, String newTopic, int queueNum, int topicSysFlag) throws MQClientException {
        this.defaultMQProducerImpl.createTopic(key, newTopic, queueNum, topicSysFlag);
    }

    /**
     * Search consume queue offset of the given time stamp.
     * 搜索给定时间戳的消耗队列偏移量。
     * @param mq Instance of MessageQueue
     * @param timestamp from when in milliseconds.
     * @return Consume queue offset.
     * @throws MQClientException if there is any client error.
     */
    @Override
    public long searchOffset(MessageQueue mq, long timestamp) throws MQClientException {
        return this.defaultMQProducerImpl.searchOffset(mq, timestamp);
    }

    /**
     * Query maximum offset of the given message queue.
     * 查询给定消息队列的最大偏移量。
     * @param mq Instance of MessageQueue
     * @return maximum offset of the given consume queue.
     * @throws MQClientException if there is any client error.
     */
    @Override
    public long maxOffset(MessageQueue mq) throws MQClientException {
        return this.defaultMQProducerImpl.maxOffset(mq);
    }

    /**
     * Query minimum offset of the given message queue.
     * 查询给定消息队列的最小偏移量
     * @param mq Instance of MessageQueue
     * @return minimum offset of the given message queue.
     * @throws MQClientException if there is any client error.
     */
    @Override
    public long minOffset(MessageQueue mq) throws MQClientException {
        return this.defaultMQProducerImpl.minOffset(mq);
    }

    /**
     * Query earliest message store time.
     * 查询最早的消息存储时间。
     * @param mq Instance of MessageQueue
     * @return earliest message store time.
     * @throws MQClientException if there is any client error.
     */
    @Override
    public long earliestMsgStoreTime(MessageQueue mq) throws MQClientException {
        return this.defaultMQProducerImpl.earliestMsgStoreTime(mq);
    }

    /**
     * Query message of the given offset message ID.
     * 查询给定偏移量消息ID的消息
     * @param offsetMsgId message id
     * @return Message specified.
     * @throws MQBrokerException if there is any broker error.
     * @throws MQClientException if there is any client error.
     * @throws RemotingException if there is any network-tier error.
     * @throws InterruptedException if the sending thread is interrupted.
     */
    @Override
    public MessageExt viewMessage(
        String offsetMsgId) throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        return this.defaultMQProducerImpl.viewMessage(offsetMsgId);
    }

    /**
     * Query message by key.
     * 按键查询消息
     * @param topic message topic
     * @param key message key index word
     * @param maxNum max message number
     * @param begin from when
     * @param end to when
     * @return QueryResult instance contains matched messages.
     * @throws MQClientException if there is any client error.
     * @throws InterruptedException if the thread is interrupted.
     */
    @Override
    public QueryResult queryMessage(String topic, String key, int maxNum, long begin, long end)
        throws MQClientException, InterruptedException {
        return this.defaultMQProducerImpl.queryMessage(topic, key, maxNum, begin, end);
    }

    /**
     * Query message of the given message ID.
     * 查询给定消息ID的消息。
     * @param topic Topic
     * @param msgId Message ID
     * @return Message specified.
     * @throws MQBrokerException if there is any broker error.
     * @throws MQClientException if there is any client error.
     * @throws RemotingException if there is any network-tier error.
     * @throws InterruptedException if the sending thread is interrupted.
     */
    @Override
    public MessageExt viewMessage(String topic,
        String msgId) throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        try {
            MessageId oldMsgId = MessageDecoder.decodeMessageId(msgId);
            return this.viewMessage(msgId);
        } catch (Exception e) {
        }
        return this.defaultMQProducerImpl.queryMessageByUniqKey(topic, msgId);
    }

    @Override
    public SendResult send(
        Collection<Message> msgs) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return this.defaultMQProducerImpl.send(batch(msgs));
    }

    @Override
    public SendResult send(Collection<Message> msgs,
        long timeout) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return this.defaultMQProducerImpl.send(batch(msgs), timeout);
    }

    @Override
    public SendResult send(Collection<Message> msgs,
        MessageQueue messageQueue) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return this.defaultMQProducerImpl.send(batch(msgs), messageQueue);
    }

    @Override
    public SendResult send(Collection<Message> msgs, MessageQueue messageQueue,
        long timeout) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return this.defaultMQProducerImpl.send(batch(msgs), messageQueue, timeout);
    }

    /**
     * Sets an Executor to be used for executing callback methods.
     * If the Executor is not set, {@link NettyRemotingClient#publicExecutor} will be used.
     * 设置用于执行回调方法的执行器。如果未设置执行器，将使用
     * @param callbackExecutor the instance of Executor
     */
    public void setCallbackExecutor(final ExecutorService callbackExecutor) {
        this.defaultMQProducerImpl.setCallbackExecutor(callbackExecutor);
    }

    /**
     * Sets an Executor to be used for executing asynchronous send. If the Executor is not set, {@link
     * DefaultMQProducerImpl#defaultAsyncSenderExecutor} will be used.
     * 设置用于执行异步发送的执行器。如果未设置执行器，将使用。
     * @param asyncSenderExecutor the instance of Executor
     */
    public void setAsyncSenderExecutor(final ExecutorService asyncSenderExecutor) {
        this.defaultMQProducerImpl.setAsyncSenderExecutor(asyncSenderExecutor);
    }

    private MessageBatch batch(Collection<Message> msgs) throws MQClientException {
        MessageBatch msgBatch;
        try {
            msgBatch = MessageBatch.generateFromList(msgs);
            for (Message message : msgBatch) {
                Validators.checkMessage(message, this);
                MessageClientIDSetter.setUniqID(message);
            }
            msgBatch.setBody(msgBatch.encode());
        } catch (Exception e) {
            throw new MQClientException("Failed to initiate the MessageBatch", e);
        }
        return msgBatch;
    }

    public String getProducerGroup() {
        return producerGroup;
    }

    public void setProducerGroup(String producerGroup) {
        this.producerGroup = producerGroup;
    }

    public String getCreateTopicKey() {
        return createTopicKey;
    }

    public void setCreateTopicKey(String createTopicKey) {
        this.createTopicKey = createTopicKey;
    }

    public int getSendMsgTimeout() {
        return sendMsgTimeout;
    }

    public void setSendMsgTimeout(int sendMsgTimeout) {
        this.sendMsgTimeout = sendMsgTimeout;
    }

    public int getCompressMsgBodyOverHowmuch() {
        return compressMsgBodyOverHowmuch;
    }

    public void setCompressMsgBodyOverHowmuch(int compressMsgBodyOverHowmuch) {
        this.compressMsgBodyOverHowmuch = compressMsgBodyOverHowmuch;
    }

    public DefaultMQProducerImpl getDefaultMQProducerImpl() {
        return defaultMQProducerImpl;
    }

    public boolean isRetryAnotherBrokerWhenNotStoreOK() {
        return retryAnotherBrokerWhenNotStoreOK;
    }

    public void setRetryAnotherBrokerWhenNotStoreOK(boolean retryAnotherBrokerWhenNotStoreOK) {
        this.retryAnotherBrokerWhenNotStoreOK = retryAnotherBrokerWhenNotStoreOK;
    }

    public int getMaxMessageSize() {
        return maxMessageSize;
    }

    public void setMaxMessageSize(int maxMessageSize) {
        this.maxMessageSize = maxMessageSize;
    }

    public int getDefaultTopicQueueNums() {
        return defaultTopicQueueNums;
    }

    public void setDefaultTopicQueueNums(int defaultTopicQueueNums) {
        this.defaultTopicQueueNums = defaultTopicQueueNums;
    }

    public int getRetryTimesWhenSendFailed() {
        return retryTimesWhenSendFailed;
    }

    public void setRetryTimesWhenSendFailed(int retryTimesWhenSendFailed) {
        this.retryTimesWhenSendFailed = retryTimesWhenSendFailed;
    }

    public boolean isSendMessageWithVIPChannel() {
        return isVipChannelEnabled();
    }

    public void setSendMessageWithVIPChannel(final boolean sendMessageWithVIPChannel) {
        this.setVipChannelEnabled(sendMessageWithVIPChannel);
    }

    public long[] getNotAvailableDuration() {
        return this.defaultMQProducerImpl.getNotAvailableDuration();
    }

    public void setNotAvailableDuration(final long[] notAvailableDuration) {
        this.defaultMQProducerImpl.setNotAvailableDuration(notAvailableDuration);
    }

    public long[] getLatencyMax() {
        return this.defaultMQProducerImpl.getLatencyMax();
    }

    public void setLatencyMax(final long[] latencyMax) {
        this.defaultMQProducerImpl.setLatencyMax(latencyMax);
    }

    public boolean isSendLatencyFaultEnable() {
        return this.defaultMQProducerImpl.isSendLatencyFaultEnable();
    }

    public void setSendLatencyFaultEnable(final boolean sendLatencyFaultEnable) {
        this.defaultMQProducerImpl.setSendLatencyFaultEnable(sendLatencyFaultEnable);
    }

    public int getRetryTimesWhenSendAsyncFailed() {
        return retryTimesWhenSendAsyncFailed;
    }

    public void setRetryTimesWhenSendAsyncFailed(final int retryTimesWhenSendAsyncFailed) {
        this.retryTimesWhenSendAsyncFailed = retryTimesWhenSendAsyncFailed;
    }

    public TraceDispatcher getTraceDispatcher() {
        return traceDispatcher;
    }

}
