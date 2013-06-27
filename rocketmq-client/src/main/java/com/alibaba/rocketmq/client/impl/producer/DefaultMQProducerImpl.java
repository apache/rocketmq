/**
 * $Id: DefaultMQProducerImpl.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.client.impl.producer;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;

import com.alibaba.rocketmq.client.QueryResult;
import com.alibaba.rocketmq.client.exception.MQBrokerException;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.impl.CommunicationMode;
import com.alibaba.rocketmq.client.impl.MQClientManager;
import com.alibaba.rocketmq.client.impl.factory.MQClientFactory;
import com.alibaba.rocketmq.client.log.ClientLogger;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.LocalTransactionExecuter;
import com.alibaba.rocketmq.client.producer.LocalTransactionState;
import com.alibaba.rocketmq.client.producer.MessageQueueSelector;
import com.alibaba.rocketmq.client.producer.SendCallback;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.client.producer.SendStatus;
import com.alibaba.rocketmq.client.producer.TransactionCheckListener;
import com.alibaba.rocketmq.client.producer.TransactionMQProducer;
import com.alibaba.rocketmq.common.MixAll;
import com.alibaba.rocketmq.common.ServiceState;
import com.alibaba.rocketmq.common.UtilALl;
import com.alibaba.rocketmq.common.help.FAQUrl;
import com.alibaba.rocketmq.common.message.Message;
import com.alibaba.rocketmq.common.message.MessageDecoder;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.message.MessageId;
import com.alibaba.rocketmq.common.message.MessageQueue;
import com.alibaba.rocketmq.common.protocol.MQProtos.MQResponseCode;
import com.alibaba.rocketmq.common.protocol.header.CheckTransactionStateRequestHeader;
import com.alibaba.rocketmq.common.protocol.header.EndTransactionRequestHeader;
import com.alibaba.rocketmq.common.protocol.header.SendMessageRequestHeader;
import com.alibaba.rocketmq.common.sysflag.MessageSysFlag;
import com.alibaba.rocketmq.remoting.common.RemotingUtil;
import com.alibaba.rocketmq.remoting.exception.RemotingException;
import com.alibaba.rocketmq.remoting.protocol.RemotingProtos.ResponseCode;


/**
 * 生产者默认实现
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 */
public class DefaultMQProducerImpl implements MQProducerInner {
    private final Logger log = ClientLogger.getLog();
    private ServiceState serviceState = ServiceState.CREATE_JUST;

    private final DefaultMQProducer defaultMQProducer;

    private final ConcurrentHashMap<String/* topic */, TopicPublishInfo> topicPublishInfoTable =
            new ConcurrentHashMap<String, TopicPublishInfo>();

    private MQClientFactory mQClientFactory;

    /**
     * 事务相关
     */
    protected BlockingQueue<Runnable> checkRequestQueue;
    protected ExecutorService checkExecutor;


    public DefaultMQProducerImpl(final DefaultMQProducer defaultMQProducer) {
        this.defaultMQProducer = defaultMQProducer;
    }


    public void initTransactionEnv() {
        TransactionMQProducer producer = (TransactionMQProducer) this.defaultMQProducer;
        this.checkRequestQueue = new LinkedBlockingQueue<Runnable>(producer.getCheckRequestHoldMax());
        this.checkExecutor = new ThreadPoolExecutor(//
            producer.getCheckThreadPoolMinSize(),//
            producer.getCheckThreadPoolMaxSize(),//
            1000 * 60,//
            TimeUnit.MILLISECONDS,//
            this.checkRequestQueue);
    }


    public void destroyTransactionEnv() {
        this.checkExecutor.shutdown();
        this.checkRequestQueue.clear();
    }


    private void checkConfig() throws MQClientException {
        if (null == this.defaultMQProducer.getProducerGroup()) {
            throw new MQClientException("producerGroup is null", null);
        }

        if (this.defaultMQProducer.getProducerGroup().equals(MixAll.DEFAULT_PRODUCER_GROUP)) {
            throw new MQClientException("producerGroup can not equal " + MixAll.DEFAULT_PRODUCER_GROUP
                    + ", please specify another one.", null);
        }
    }


    public void start() throws MQClientException {
        switch (this.serviceState) {
        case CREATE_JUST:
            this.checkConfig();

            this.serviceState = ServiceState.RUNNING;

            this.mQClientFactory =
                    MQClientManager.getInstance().getAndCreateMQClientFactory(this.defaultMQProducer);

            boolean registerOK =
                    mQClientFactory.registerProducer(this.defaultMQProducer.getProducerGroup(), this);
            if (!registerOK) {
                this.serviceState = ServiceState.CREATE_JUST;
                throw new MQClientException("The producer group[" + this.defaultMQProducer.getProducerGroup()
                        + "] has created already, specifed another name please."//
                        + FAQUrl.suggestTodo(FAQUrl.GROUP_NAME_DUPLICATE_URL), null);
            }

            // 默认Topic注册
            this.topicPublishInfoTable
                .put(this.defaultMQProducer.getCreateTopicKey(), new TopicPublishInfo());

            mQClientFactory.start();
            log.info("the producer [{}] start OK", this.defaultMQProducer.getProducerGroup());
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
            this.mQClientFactory.unregisterProducer(this.defaultMQProducer.getProducerGroup());
            this.mQClientFactory.shutdown();
            log.info("the producer [{}] shutdown OK", this.defaultMQProducer.getProducerGroup());
            this.serviceState = ServiceState.SHUTDOWN_ALREADY;
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


    @Override
    public void updateTopicPublishInfo(final String topic, final TopicPublishInfo info) {
        if (info != null && topic != null) {
            TopicPublishInfo prev = this.topicPublishInfoTable.put(topic, info);
            if (prev != null) {
                log.info("updateTopicPublishInfo prev is not null, " + prev.toString());
            }
        }
    }


    @Override
    public Set<String> getPublishTopicList() {
        Set<String> topicList = new HashSet<String>();
        for (String key : this.topicPublishInfoTable.keySet()) {
            topicList.add(key);
        }

        return topicList;
    }


    public void createTopic(String key, String newTopic, int queueNum, boolean order)
            throws MQClientException {
        this.makeSureStateOK();
        this.mQClientFactory.getMQAdminImpl().createTopic(key, newTopic, queueNum, order);
    }


    public List<MessageQueue> fetchPublishMessageQueues(String topic) throws MQClientException {
        this.makeSureStateOK();
        return this.mQClientFactory.getMQAdminImpl().fetchPublishMessageQueues(topic);
    }


    public long searchOffset(MessageQueue mq, long timestamp) throws MQClientException {
        this.makeSureStateOK();
        return this.mQClientFactory.getMQAdminImpl().searchOffset(mq, timestamp);
    }


    public long maxOffset(MessageQueue mq) throws MQClientException {
        this.makeSureStateOK();
        return this.mQClientFactory.getMQAdminImpl().maxOffset(mq);
    }


    public long minOffset(MessageQueue mq) throws MQClientException {
        this.makeSureStateOK();
        return this.mQClientFactory.getMQAdminImpl().minOffset(mq);
    }


    public long earliestMsgStoreTime(MessageQueue mq) throws MQClientException {
        this.makeSureStateOK();
        return this.mQClientFactory.getMQAdminImpl().earliestMsgStoreTime(mq);
    }


    public MessageExt viewMessage(String msgId) throws RemotingException, MQBrokerException,
            InterruptedException, MQClientException {
        this.makeSureStateOK();

        return this.mQClientFactory.getMQAdminImpl().viewMessage(msgId);
    }


    public QueryResult queryMessage(String topic, String key, int maxNum, long begin, long end)
            throws MQClientException, InterruptedException {
        this.makeSureStateOK();
        return this.mQClientFactory.getMQAdminImpl().queryMessage(topic, key, maxNum, begin, end);
    }


    private void checkMessage(Message msg) throws MQClientException {
        if (null == msg) {
            throw new MQClientException("the message is null", null);
        }
        // topic
        if (null == msg.getTopic()) {
            throw new MQClientException("the message topic is null", null);
        }
        // body
        if (null == msg.getBody()) {
            throw new MQClientException("the message body is null", null);
        }

        if (0 == msg.getBody().length) {
            throw new MQClientException("the message body length is zero", null);
        }

        if (msg.getBody().length > this.defaultMQProducer.getMaxMessageSize()) {
            throw new MQClientException("the message body size over max value, MAX: "
                    + this.defaultMQProducer.getMaxMessageSize(), null);
        }
    }


    private boolean tryToCompressMessage(final Message msg) {
        byte[] body = msg.getBody();
        if (body != null) {
            if (body.length >= this.defaultMQProducer.getCompressMsgBodyOverHowmuch()) {
                try {
                    byte[] data = UtilALl.compress(body, 9);
                    if (data != null) {
                        msg.setBody(data);
                        return true;
                    }
                }
                catch (IOException e) {
                    log.error("tryToCompressMessage exception", e);
                    log.warn(msg.toString());
                }
            }
        }

        return false;
    }


    private SendResult sendDefaultImpl(//
            Message msg,//
            final CommunicationMode communicationMode,//
            final SendCallback sendCallback//
    ) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        final long beginTimestamp = System.currentTimeMillis();
        long endTimestamp = beginTimestamp;
        TopicPublishInfo topicPublishInfo = this.tryToFindTopicPublishInfo(msg.getTopic());
        if (topicPublishInfo != null && topicPublishInfo.ok()) {
            MessageQueue mq = null;
            Exception exception = null;
            SendResult sendResult = null;
            for (int times = 0; times < 3
                    && (endTimestamp - beginTimestamp) < this.defaultMQProducer.getSendMsgTimeout(); times++) {
                String lastBrokerName = null == mq ? null : mq.getBrokerName();
                mq = topicPublishInfo.selectOneMessageQueue(lastBrokerName);
                if (mq != null) {
                    try {
                        sendResult = this.sendKernelImpl(msg, mq, communicationMode, sendCallback);
                        endTimestamp = System.currentTimeMillis();
                        switch (communicationMode) {
                        case ASYNC:
                            return null;
                        case ONEWAY:
                            return null;
                        case SYNC:
                            if (sendResult.getSendStatus() != SendStatus.SEND_OK) {
                                if (this.defaultMQProducer.isRetryAnotherBrokerWhenNotStoreOK()) {
                                    continue;
                                }
                            }

                            return sendResult;
                        default:
                            break;
                        }
                    }
                    catch (RemotingException e) {
                        log.warn("sendKernelImpl exception", e);
                        log.warn(msg.toString());
                        exception = e;
                        endTimestamp = System.currentTimeMillis();
                        continue;
                    }
                    catch (MQClientException e) {
                        log.warn("sendKernelImpl exception", e);
                        log.warn(msg.toString());
                        exception = e;
                        endTimestamp = System.currentTimeMillis();
                        continue;
                    }
                    catch (MQBrokerException e) {
                        log.warn("sendKernelImpl exception", e);
                        log.warn(msg.toString());
                        exception = e;
                        endTimestamp = System.currentTimeMillis();
                        switch (e.getResponseCode()) {
                        case MQResponseCode.TOPIC_NOT_EXIST_VALUE:
                        case MQResponseCode.SERVICE_NOT_AVAILABLE_VALUE:
                        case ResponseCode.SYSTEM_ERROR_VALUE:
                        case MQResponseCode.NO_PERMISSION_VALUE:
                            continue;
                        default:
                            if (sendResult != null) {
                                return sendResult;
                            }

                            throw e;
                        }
                    }
                    catch (InterruptedException e) {
                        log.warn("sendKernelImpl exception", e);
                        log.warn(msg.toString());
                        throw e;
                    }
                }
                else {
                    break;
                }
            } // end of for

            if (sendResult != null) {
                return sendResult;
            }

            throw new MQClientException("Retry many times, still failed", exception);
        }

        throw new MQClientException("No route info of this topic, " + msg.getTopic(), null);
    }


    private SendResult sendKernelImpl(final Message msg,//
            final MessageQueue mq,//
            final CommunicationMode communicationMode,//
            final SendCallback sendCallback//
    ) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        String brokerAddr = this.mQClientFactory.findBrokerAddressInPublish(mq.getBrokerName());
        if (null == brokerAddr) {
            // TODO 此处可能对Name Server压力过大，需要调优
            this.mQClientFactory.updateTopicRouteInfoFromNameServer(mq.getTopic());
            this.mQClientFactory.updateTopicRouteInfoFromNameServer(this.defaultMQProducer
                .getCreateTopicKey());
            brokerAddr = this.mQClientFactory.findBrokerAddressInPublish(mq.getBrokerName());
        }

        if (brokerAddr != null) {
            byte[] prevBody = msg.getBody();
            try {
                int sysFlag = 0;
                if (this.tryToCompressMessage(msg)) {
                    sysFlag |= MessageSysFlag.CompressedFlag;
                }

                final String tranMsg = msg.getProperty(Message.PROPERTY_TRANSACTION_PREPARED);
                if (tranMsg != null && Boolean.parseBoolean(tranMsg)) {
                    sysFlag |= MessageSysFlag.TransactionPreparedType;
                }

                SendMessageRequestHeader requestHeader = new SendMessageRequestHeader();
                requestHeader.setProducerGroup(this.defaultMQProducer.getProducerGroup());
                requestHeader.setTopic(msg.getTopic());
                requestHeader.setDefaultTopic(this.defaultMQProducer.getCreateTopicKey());
                requestHeader.setDefaultTopicQueueNums(this.defaultMQProducer.getDefaultTopicQueueNums());
                requestHeader.setQueueId(mq.getQueueId());
                requestHeader.setSysFlag(sysFlag);
                requestHeader.setBornTimestamp(System.currentTimeMillis());
                requestHeader.setFlag(msg.getFlag());
                requestHeader.setProperties(MessageDecoder.messageProperties2String(msg.getProperties()));

                SendResult sendResult = this.mQClientFactory.getMQClientAPIImpl().sendMessage(//
                    brokerAddr,// 1
                    mq.getBrokerName(),// 2
                    msg,// 3
                    requestHeader,// 4
                    this.defaultMQProducer.getSendMsgTimeout(),// 5
                    communicationMode,// 6
                    sendCallback// 7
                    );

                return sendResult;
            }
            finally {
                msg.setBody(prevBody);
            }
        }

        throw new MQClientException("The broker[" + mq.getBrokerName() + "] not exist", null);
    }


    private SendResult sendSelectImpl(//
            Message msg,//
            MessageQueueSelector selector,//
            Object arg,//
            final CommunicationMode communicationMode,//
            final SendCallback sendCallback//
    ) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        TopicPublishInfo topicPublishInfo = this.tryToFindTopicPublishInfo(msg.getTopic());
        if (topicPublishInfo != null && topicPublishInfo.ok()) {
            MessageQueue mq = null;
            try {
                mq = selector.select(topicPublishInfo.getMessageQueueList(), msg, arg);
            }
            catch (Throwable e) {
                throw new MQClientException("select message queue throwed exception.", e);
            }

            if (mq != null) {
                return this.sendKernelImpl(msg, mq, communicationMode, sendCallback);
            }
            else {
                throw new MQClientException("select message queue return null.", null);
            }
        }

        throw new MQClientException("No route info for this topic, " + msg.getTopic(), null);
    }


    /**
     * 尝试寻找Topic路由信息，如果没有则到Name Server上找，再没有，则取默认Topic
     */
    private TopicPublishInfo tryToFindTopicPublishInfo(final String topic) {
        TopicPublishInfo topicPublishInfo = this.topicPublishInfoTable.get(topic);
        if (null == topicPublishInfo) {
            this.topicPublishInfoTable.putIfAbsent(topic, new TopicPublishInfo());
            this.mQClientFactory.updateTopicRouteInfoFromNameServer(topic);
            this.mQClientFactory.updateTopicRouteInfoFromNameServer(this.defaultMQProducer
                .getCreateTopicKey());
            topicPublishInfo = this.topicPublishInfoTable.get(topic);
        }

        if (topicPublishInfo != null && topicPublishInfo.ok()) {
            return topicPublishInfo;
        }

        return this.topicPublishInfoTable.get(this.defaultMQProducer.getCreateTopicKey());
    }


    /**
     * DEFAULT SYNC -------------------------------------------------------
     */
    public SendResult send(Message msg) throws MQClientException, RemotingException, MQBrokerException,
            InterruptedException {
        this.makeSureStateOK();

        this.checkMessage(msg);

        return this.sendDefaultImpl(msg, CommunicationMode.SYNC, null);
    }


    /**
     * DEFAULT ASYNC -------------------------------------------------------
     */
    public void send(Message msg, SendCallback sendCallback) throws MQClientException, RemotingException,
            InterruptedException {
        this.makeSureStateOK();

        this.checkMessage(msg);

        try {
            this.sendDefaultImpl(msg, CommunicationMode.ASYNC, sendCallback);
        }
        catch (MQBrokerException e) {
            throw new MQClientException("unknow exception", e);
        }
    }


    /**
     * DEFAULT ONEWAY -------------------------------------------------------
     */
    public void sendOneway(Message msg) throws MQClientException, RemotingException, InterruptedException {
        this.makeSureStateOK();

        this.checkMessage(msg);

        try {
            this.sendDefaultImpl(msg, CommunicationMode.ONEWAY, null);
        }
        catch (MQBrokerException e) {
            throw new MQClientException("unknow exception", e);
        }
    }


    /**
     * KERNEL SYNC -------------------------------------------------------
     */
    public SendResult send(Message msg, MessageQueue mq) throws MQClientException, RemotingException,
            MQBrokerException, InterruptedException {
        this.makeSureStateOK();

        this.checkMessage(msg);

        return this.sendKernelImpl(msg, mq, CommunicationMode.SYNC, null);
    }


    /**
     * KERNEL ASYNC -------------------------------------------------------
     */
    public void send(Message msg, MessageQueue mq, SendCallback sendCallback) throws MQClientException,
            RemotingException, InterruptedException {
        this.makeSureStateOK();

        this.checkMessage(msg);

        try {
            this.sendKernelImpl(msg, mq, CommunicationMode.ASYNC, sendCallback);
        }
        catch (MQBrokerException e) {
            throw new MQClientException("unknow exception", e);
        }
    }


    /**
     * KERNEL ONEWAY -------------------------------------------------------
     */
    public void sendOneway(Message msg, MessageQueue mq) throws MQClientException, RemotingException,
            InterruptedException {
        this.makeSureStateOK();

        this.checkMessage(msg);

        try {
            this.sendKernelImpl(msg, mq, CommunicationMode.ONEWAY, null);
        }
        catch (MQBrokerException e) {
            throw new MQClientException("unknow exception", e);
        }
    }


    /**
     * SELECT SYNC -------------------------------------------------------
     */
    public SendResult send(Message msg, MessageQueueSelector selector, Object arg) throws MQClientException,
            RemotingException, MQBrokerException, InterruptedException {
        return this.sendSelectImpl(msg, selector, arg, CommunicationMode.SYNC, null);
    }


    /**
     * SELECT ASYNC -------------------------------------------------------
     */
    public void send(Message msg, MessageQueueSelector selector, Object arg, SendCallback sendCallback)
            throws MQClientException, RemotingException, InterruptedException {
        try {
            this.sendSelectImpl(msg, selector, arg, CommunicationMode.ASYNC, sendCallback);
        }
        catch (MQBrokerException e) {
            throw new MQClientException("unknow exception", e);
        }
    }


    /**
     * SELECT ONEWAY -------------------------------------------------------
     */
    public void sendOneway(Message msg, MessageQueueSelector selector, Object arg) throws MQClientException,
            RemotingException, InterruptedException {
        try {
            this.sendSelectImpl(msg, selector, arg, CommunicationMode.ONEWAY, null);
        }
        catch (MQBrokerException e) {
            throw new MQClientException("unknow exception", e);
        }
    }


    private void endTransaction(//
            final SendResult sendResult, //
            final LocalTransactionState localTransactionState, //
            final Throwable localException) throws RemotingException, MQBrokerException,
            InterruptedException, UnknownHostException {
        final MessageId id = MessageDecoder.decodeMessageId(sendResult.getMsgId());
        final String addr = RemotingUtil.socketAddress2String(id.getAddress());
        EndTransactionRequestHeader requestHeader = new EndTransactionRequestHeader();
        requestHeader.setCommitLogOffset(id.getOffset());
        switch (localTransactionState) {
        case COMMIT_MESSAGE:
            requestHeader.setCommitOrRollback(MessageSysFlag.TransactionCommitType);
            break;
        case ROLLBACK_MESSAGE:
            requestHeader.setCommitOrRollback(MessageSysFlag.TransactionRollbackType);
            break;
        case UNKNOW:
            requestHeader.setCommitOrRollback(MessageSysFlag.TransactionNotType);
            break;
        default:
            break;
        }

        requestHeader.setProducerGroup(this.defaultMQProducer.getProducerGroup());
        requestHeader.setTranStateTableOffset(sendResult.getQueueOffset());
        requestHeader.setMsgId(sendResult.getMsgId());
        String remark =
                localException != null ? ("executeLocalTransactionBranch exception: " + localException
                    .toString()) : null;
        this.mQClientFactory.getMQClientAPIImpl().endTransactionOneway(addr, requestHeader, remark,
            this.defaultMQProducer.getSendMsgTimeout());
    }


    public SendResult sendMessageInTransaction(final Message msg, final LocalTransactionExecuter tranExecuter)
            throws MQClientException {
        if (null == msg) {
            throw new MQClientException("msg is null", null);
        }

        if (null == tranExecuter) {
            throw new MQClientException("tranExecuter is null", null);
        }

        // 第一步，向Broker发送一条Prepared消息
        SendResult sendResult = null;
        msg.putProperty(Message.PROPERTY_TRANSACTION_PREPARED, "true");
        msg.putProperty(Message.PROPERTY_PRODUCER_GROUP, this.defaultMQProducer.getProducerGroup());
        try {
            sendResult = this.send(msg);
        }
        catch (Exception e) {
            throw new MQClientException("send message Exception", e);
        }

        // 第二步，回调本地事务
        LocalTransactionState localTransactionState = LocalTransactionState.UNKNOW;
        Throwable localException = null;
        try {
            localTransactionState = tranExecuter.executeLocalTransactionBranch(msg);
            if (null == localTransactionState) {
                localTransactionState = LocalTransactionState.UNKNOW;
            }

            if (localTransactionState != LocalTransactionState.COMMIT_MESSAGE) {
                log.info("executeLocalTransactionBranch return {}", localTransactionState);
                log.info(msg.toString());
            }
        }
        catch (Throwable e) {
            log.info("executeLocalTransactionBranch exception", e);
            log.info(msg.toString());
            localException = e;
        }

        // 第三步，提交或者回滚Broker端消息
        try {
            this.endTransaction(sendResult, localTransactionState, localException);
        }
        catch (Exception e) {
            log.warn("local transaction execute " + localTransactionState
                    + ", but end broker transaction failed", e);
        }

        return sendResult;
    }


    @Override
    public TransactionCheckListener checkListener() {
        if (this.defaultMQProducer instanceof TransactionMQProducer) {
            TransactionMQProducer producer = (TransactionMQProducer) defaultMQProducer;
            return producer.getTransactionCheckListener();
        }

        return null;
    }


    @Override
    public void checkTransactionState(final String addr, final MessageExt msg,
            final CheckTransactionStateRequestHeader header) {
        Runnable request = new Runnable() {
            private final String brokerAddr = addr;
            private final MessageExt message = msg;
            private final CheckTransactionStateRequestHeader checkRequestHeader = header;
            private final String group = DefaultMQProducerImpl.this.defaultMQProducer.getProducerGroup();


            private void processTransactionState(//
                    final LocalTransactionState localTransactionState,//
                    final String producerGroup,//
                    final Throwable exception) {
                final EndTransactionRequestHeader thisHeader = new EndTransactionRequestHeader();
                thisHeader.setCommitLogOffset(checkRequestHeader.getCommitLogOffset());
                thisHeader.setProducerGroup(producerGroup);
                thisHeader.setTranStateTableOffset(checkRequestHeader.getTranStateTableOffset());
                thisHeader.setFromTransactionCheck(true);
                thisHeader.setMsgId(message.getMsgId());
                switch (localTransactionState) {
                case COMMIT_MESSAGE:
                    thisHeader.setCommitOrRollback(MessageSysFlag.TransactionCommitType);
                    break;
                case ROLLBACK_MESSAGE:
                    thisHeader.setCommitOrRollback(MessageSysFlag.TransactionRollbackType);
                    log.warn("when broker check, client rollback this transaction, {}", thisHeader);
                    break;
                case UNKNOW:
                    thisHeader.setCommitOrRollback(MessageSysFlag.TransactionNotType);
                    log.warn("when broker check, client donot know this transaction state, {}", thisHeader);
                    break;
                default:
                    break;
                }

                String remark = null;
                if (exception != null) {
                    remark = "checkLocalTransactionState Exception: " + exception.toString();
                }

                try {
                    DefaultMQProducerImpl.this.mQClientFactory.getMQClientAPIImpl().endTransactionOneway(
                        brokerAddr, thisHeader, remark, 3000);
                }
                catch (Exception e) {
                    log.error("endTransactionOneway exception", e);
                }
            }


            @Override
            public void run() {
                TransactionCheckListener transactionCheckListener =
                        DefaultMQProducerImpl.this.checkListener();
                if (transactionCheckListener != null) {
                    LocalTransactionState localTransactionState = LocalTransactionState.UNKNOW;
                    Throwable exception = null;
                    try {
                        localTransactionState = transactionCheckListener.checkLocalTransactionState(message);
                    }
                    catch (Throwable e) {
                        log.error(
                            "Broker call checkTransactionState, but checkLocalTransactionState exception", e);
                        exception = e;
                    }

                    this.processTransactionState(//
                        localTransactionState,//
                        group, //
                        exception);
                }
                else {
                    log.warn("checkTransactionState, pick transactionCheckListener by group[{}] failed",
                        group);
                }
            }
        };

        this.checkExecutor.submit(request);
    }
}
