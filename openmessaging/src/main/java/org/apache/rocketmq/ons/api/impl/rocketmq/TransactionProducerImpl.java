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

package org.apache.rocketmq.ons.api.impl.rocketmq;

import io.openmessaging.api.Message;
import io.openmessaging.api.SendResult;
import io.openmessaging.api.transaction.LocalTransactionExecuter;
import io.openmessaging.api.transaction.TransactionProducer;
import io.openmessaging.api.transaction.TransactionStatus;
import java.util.Properties;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.acl.common.AclClientRPCHook;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.TransactionCheckListener;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.ons.api.Constants;
import org.apache.rocketmq.ons.api.PropertyKeyConst;
import org.apache.rocketmq.ons.api.impl.util.ClientLoggerUtil;
import org.apache.rocketmq.ons.open.trace.core.common.OnsTraceConstants;
import org.apache.rocketmq.ons.open.trace.core.common.OnsTraceDispatcherType;
import org.apache.rocketmq.ons.open.trace.core.dispatch.impl.AsyncArrayDispatcher;
import org.apache.rocketmq.ons.open.trace.core.hook.OnsClientSendMessageHookImpl;
import org.apache.rocketmq.remoting.protocol.LanguageCode;

public class TransactionProducerImpl extends ONSClientAbstract implements TransactionProducer {
    private final static InternalLogger LOGGER = ClientLoggerUtil.getClientLogger();
    TransactionMQProducer transactionMQProducer = null;
    private Properties properties;

    public TransactionProducerImpl(Properties properties, TransactionCheckListener transactionCheckListener) {
        super(properties);
        this.properties = properties;
        String producerGroup = properties.getProperty(PropertyKeyConst.GROUP_ID, properties.getProperty(PropertyKeyConst.GROUP_ID));
        if (StringUtils.isEmpty(producerGroup)) {
            producerGroup = "__ONS_PRODUCER_DEFAULT_GROUP";
        }
        transactionMQProducer =
            new TransactionMQProducer(this.getNamespace(), producerGroup, new OnsClientRPCHook(sessionCredentials));

        boolean isVipChannelEnabled = Boolean.parseBoolean(properties.getProperty(PropertyKeyConst.isVipChannelEnabled, "false"));
        transactionMQProducer.setVipChannelEnabled(isVipChannelEnabled);
        if (properties.containsKey(PropertyKeyConst.LANGUAGE_IDENTIFIER)) {
            int language = Integer.valueOf(properties.get(PropertyKeyConst.LANGUAGE_IDENTIFIER).toString());
            byte languageByte = (byte) language;
            this.transactionMQProducer.setLanguage(LanguageCode.valueOf(languageByte));
        }
        String instanceName = properties.getProperty(PropertyKeyConst.InstanceName, this.buildIntanceName());
        this.transactionMQProducer.setInstanceName(instanceName);

//        boolean addExtendUniqInfo = Boolean.parseBoolean(properties.getProperty(PropertyKeyConst.EXACTLYONCE_DELIVERY, "false"));
//        transactionMQProducer.setAddExtendUniqInfo(addExtendUniqInfo);

        transactionMQProducer.setTransactionCheckListener(transactionCheckListener);
        String msgTraceSwitch = properties.getProperty(PropertyKeyConst.MsgTraceSwitch);
        if (!UtilAll.isBlank(msgTraceSwitch) && (!Boolean.parseBoolean(msgTraceSwitch))) {
            LOGGER.info("MQ Client Disable the Trace Hook!");
        } else {
            try {
                Properties tempProperties = new Properties();
                tempProperties.put(OnsTraceConstants.MaxMsgSize, "128000");
                tempProperties.put(OnsTraceConstants.AsyncBufferSize, "2048");
                tempProperties.put(OnsTraceConstants.MaxBatchNum, "100");
                tempProperties.put(OnsTraceConstants.NAMESRV_ADDR, this.getNameServerAddr());
                tempProperties.put(OnsTraceConstants.InstanceName, "PID_CLIENT_INNER_TRACE_PRODUCER");
                tempProperties.put(OnsTraceConstants.TraceDispatcherType, OnsTraceDispatcherType.PRODUCER.name());
                AsyncArrayDispatcher dispatcher = new AsyncArrayDispatcher(tempProperties, new AclClientRPCHook(sessionCredentials));
                dispatcher.setHostProducer(transactionMQProducer.getDefaultMQProducerImpl());
                traceDispatcher = dispatcher;
                this.transactionMQProducer.getDefaultMQProducerImpl().registerSendMessageHook(
                    new OnsClientSendMessageHookImpl(traceDispatcher));
            } catch (Throwable e) {
                LOGGER.error("system mqtrace hook init failed ,maybe can't send msg trace data", e);
            }
        }
    }

    @Override
    public void start() {
        if (started.compareAndSet(false, true)) {
            if (transactionMQProducer.getTransactionCheckListener() == null) {
                throw new IllegalArgumentException("TransactionCheckListener is null");
            }
            transactionMQProducer.setNamesrvAddr(this.nameServerAddr);
            try {
                transactionMQProducer.start();
                super.start();
            } catch (MQClientException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    protected void updateNameServerAddr(String newAddrs) {
        this.transactionMQProducer.getDefaultMQProducerImpl().getmQClientFactory().getMQClientAPIImpl().updateNameServerAddressList(newAddrs);
    }

    @Override
    public void shutdown() {
        if (started.compareAndSet(true, false)) {
            transactionMQProducer.shutdown();
        }
        super.shutdown();
    }

    @Override
    public SendResult send(final Message message, final LocalTransactionExecuter executer, Object arg) {
        this.checkONSProducerServiceState(this.transactionMQProducer.getDefaultMQProducerImpl());
        org.apache.rocketmq.common.message.Message msgRMQ = ONSUtil.msgConvert(message);
        org.apache.rocketmq.client.producer.TransactionSendResult sendResultRMQ = null;
        try {
            sendResultRMQ = transactionMQProducer.sendMessageInTransaction(msgRMQ,
                new org.apache.rocketmq.client.producer.LocalTransactionExecuter() {
                    @Override
                    public LocalTransactionState executeLocalTransactionBranch(
                        org.apache.rocketmq.common.message.Message msg,
                        Object arg) {
                        String msgId = msg.getProperty(Constants.TRANSACTION_ID);
                        message.setMsgID(msgId);
                        TransactionStatus transactionStatus = executer.execute(message, arg);
                        if (TransactionStatus.CommitTransaction == transactionStatus) {
                            return LocalTransactionState.COMMIT_MESSAGE;
                        } else if (TransactionStatus.RollbackTransaction == transactionStatus) {
                            return LocalTransactionState.ROLLBACK_MESSAGE;
                        }
                        return LocalTransactionState.UNKNOW;
                    }
                }, arg);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        if (sendResultRMQ.getLocalTransactionState() == LocalTransactionState.ROLLBACK_MESSAGE) {
            throw new RuntimeException("local transaction branch failed ,so transaction rollback");
        }
        SendResult sendResult = new SendResult();
        sendResult.setMessageId(sendResultRMQ.getMsgId());
        sendResult.setTopic(sendResultRMQ.getMessageQueue().getTopic());
        return sendResult;
    }

}
