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
import io.openmessaging.api.OnExceptionContext;
import io.openmessaging.api.Producer;
import io.openmessaging.api.SendCallback;
import io.openmessaging.api.SendResult;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.message.MessageClientIDSetter;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.ons.api.PropertyKeyConst;
import org.apache.rocketmq.ons.api.exception.ONSClientException;
import org.apache.rocketmq.ons.api.impl.util.ClientLoggerUtil;
import org.apache.rocketmq.ons.open.trace.core.common.OnsTraceConstants;
import org.apache.rocketmq.ons.open.trace.core.common.OnsTraceDispatcherType;
import org.apache.rocketmq.ons.open.trace.core.dispatch.impl.AsyncArrayDispatcher;
import org.apache.rocketmq.ons.open.trace.core.hook.OnsClientSendMessageHookImpl;
import org.apache.rocketmq.remoting.exception.RemotingConnectException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.protocol.LanguageCode;

public class ProducerImpl extends ONSClientAbstract implements Producer {
    private final static InternalLogger LOGGER = ClientLoggerUtil.getClientLogger();
    private final DefaultMQProducer defaultMQProducer;

    public ProducerImpl(final Properties properties) {
        super(properties);

        String producerGroup = properties.getProperty(PropertyKeyConst.GROUP_ID, properties.getProperty(PropertyKeyConst.GROUP_ID));
        if (StringUtils.isEmpty(producerGroup)) {
            producerGroup = "__ONS_PRODUCER_DEFAULT_GROUP";
        }

        this.defaultMQProducer =
            new DefaultMQProducer(this.getNamespace(), producerGroup, new OnsClientRPCHook(sessionCredentials));

        this.defaultMQProducer.setProducerGroup(producerGroup);

        boolean isVipChannelEnabled = Boolean.parseBoolean(properties.getProperty(PropertyKeyConst.isVipChannelEnabled, "false"));
        this.defaultMQProducer.setVipChannelEnabled(isVipChannelEnabled);

        if (properties.containsKey(PropertyKeyConst.SendMsgTimeoutMillis)) {
            this.defaultMQProducer.setSendMsgTimeout(Integer.valueOf(properties.get(PropertyKeyConst.SendMsgTimeoutMillis).toString()));
        } else {
            this.defaultMQProducer.setSendMsgTimeout(5000);
        }

//        if (properties.containsKey(PropertyKeyConst.EXACTLYONCE_DELIVERY)) {
//            this.defaultMQProducer.setAddExtendUniqInfo(Boolean.valueOf(properties.get(PropertyKeyConst.EXACTLYONCE_DELIVERY).toString()));
//        }

        if (properties.containsKey(PropertyKeyConst.LANGUAGE_IDENTIFIER)) {
            int language = Integer.valueOf(properties.get(PropertyKeyConst.LANGUAGE_IDENTIFIER).toString());
            byte languageByte = (byte) language;
            this.defaultMQProducer.setLanguage(LanguageCode.valueOf(languageByte));
        }
        String instanceName = properties.getProperty(PropertyKeyConst.InstanceName, this.buildIntanceName());
        this.defaultMQProducer.setInstanceName(instanceName);
        this.defaultMQProducer.setNamesrvAddr(this.getNameServerAddr());
        this.defaultMQProducer.setMaxMessageSize(1024 * 1024 * 4);
        String msgTraceSwitch = properties.getProperty(PropertyKeyConst.MsgTraceSwitch);
        if (!UtilAll.isBlank(msgTraceSwitch) && (!Boolean.parseBoolean(msgTraceSwitch))) {
            LOGGER.info("MQ Client Disable the Trace Hook!");
        } else {
            try {
                Properties tempProperties = new Properties();
                tempProperties.put(OnsTraceConstants.AccessKey, sessionCredentials.getAccessKey());
                tempProperties.put(OnsTraceConstants.SecretKey, sessionCredentials.getSecretKey());
                tempProperties.put(OnsTraceConstants.MaxMsgSize, "128000");
                tempProperties.put(OnsTraceConstants.AsyncBufferSize, "2048");
                tempProperties.put(OnsTraceConstants.MaxBatchNum, "100");
                tempProperties.put(OnsTraceConstants.NAMESRV_ADDR, this.getNameServerAddr());
                tempProperties.put(OnsTraceConstants.InstanceName, "PID_CLIENT_INNER_TRACE_PRODUCER");
                tempProperties.put(OnsTraceConstants.TraceDispatcherType, OnsTraceDispatcherType.PRODUCER.name());
                AsyncArrayDispatcher dispatcher = new AsyncArrayDispatcher(tempProperties,
                    new OnsClientRPCHook(sessionCredentials));
                dispatcher.setHostProducer(defaultMQProducer.getDefaultMQProducerImpl());
                traceDispatcher = dispatcher;
                this.defaultMQProducer.getDefaultMQProducerImpl().registerSendMessageHook(
                    new OnsClientSendMessageHookImpl(traceDispatcher));
            } catch (Throwable e) {
                LOGGER.error("system mqtrace hook init failed ,maybe can't send msg trace data.", e);
            }
        }
    }

    @Override
    protected void updateNameServerAddr(String newAddrs) {
        this.defaultMQProducer.getDefaultMQProducerImpl().getmQClientFactory().getMQClientAPIImpl().updateNameServerAddressList(newAddrs);
    }

    @Override
    public void start() {
        try {
            if (this.started.compareAndSet(false, true)) {
                this.defaultMQProducer.start();
                super.start();
            }
        } catch (Exception e) {
            throw new ONSClientException(e.getMessage());
        }
    }

    @Override
    public void shutdown() {
        if (this.started.compareAndSet(true, false)) {
            this.defaultMQProducer.shutdown();
        }
        super.shutdown();
    }

    @Override
    public SendResult send(Message message) {
        this.checkONSProducerServiceState(this.defaultMQProducer.getDefaultMQProducerImpl());
        org.apache.rocketmq.common.message.Message msgRMQ = ONSUtil.msgConvert(message);

        try {
            org.apache.rocketmq.client.producer.SendResult sendResultRMQ = this.defaultMQProducer.send(msgRMQ);

            message.setMsgID(sendResultRMQ.getMsgId());
            SendResult sendResult = new SendResult();
            sendResult.setTopic(sendResultRMQ.getMessageQueue().getTopic());
            sendResult.setMessageId(sendResultRMQ.getMsgId());
            return sendResult;
        } catch (Exception e) {
            LOGGER.error(String.format("Send message Exception, %s", message), e);
            throw checkProducerException(message.getTopic(), message.getMsgID(), e);
        }
    }

    @Override
    public void sendOneway(Message message) {
        this.checkONSProducerServiceState(this.defaultMQProducer.getDefaultMQProducerImpl());
        org.apache.rocketmq.common.message.Message msgRMQ = ONSUtil.msgConvert(message);
        try {
            this.defaultMQProducer.sendOneway(msgRMQ);
            message.setMsgID(MessageClientIDSetter.getUniqID(msgRMQ));
        } catch (Exception e) {
            LOGGER.error(String.format("Send message oneway Exception, %s", message), e);
            throw checkProducerException(message.getTopic(), message.getMsgID(), e);
        }
    }

    @Override
    public void sendAsync(Message message, SendCallback sendCallback) {
        this.checkONSProducerServiceState(this.defaultMQProducer.getDefaultMQProducerImpl());
        org.apache.rocketmq.common.message.Message msgRMQ = ONSUtil.msgConvert(message);
        try {
            this.defaultMQProducer.send(msgRMQ, sendCallbackConvert(message, sendCallback));
            message.setMsgID(MessageClientIDSetter.getUniqID(msgRMQ));
        } catch (Exception e) {
            LOGGER.error(String.format("Send message async Exception, %s", message), e);
            throw checkProducerException(message.getTopic(), message.getMsgID(), e);
        }
    }

    @Override
    public void setCallbackExecutor(final ExecutorService callbackExecutor) {
        this.defaultMQProducer.setCallbackExecutor(callbackExecutor);
    }

    public DefaultMQProducer getDefaultMQProducer() {
        return defaultMQProducer;
    }

    private org.apache.rocketmq.client.producer.SendCallback sendCallbackConvert(final Message message,
        final SendCallback sendCallback) {
        org.apache.rocketmq.client.producer.SendCallback rmqSendCallback = new org.apache.rocketmq.client.producer.SendCallback() {
            @Override
            public void onSuccess(org.apache.rocketmq.client.producer.SendResult sendResult) {
                sendCallback.onSuccess(sendResultConvert(sendResult));
            }

            @Override
            public void onException(Throwable e) {
                String topic = new String(message.getTopic());
                String msgId = new String(message.getMsgID());
                LOGGER.error(String.format("Send message async Exception, %s", message), e);
                ONSClientException onsEx = checkProducerException(topic, msgId, e);
                OnExceptionContext context = new OnExceptionContext();
                context.setTopic(message.getTopic());
                context.setMessageId(message.getMsgID());
                context.setException(onsEx);
                sendCallback.onException(context);
            }
        };
        return rmqSendCallback;
    }

    private SendResult sendResultConvert(
        final org.apache.rocketmq.client.producer.SendResult rmqSendResult) {
        SendResult sendResult = new SendResult();
        sendResult.setTopic(rmqSendResult.getMessageQueue().getTopic());
        sendResult.setMessageId(rmqSendResult.getMsgId());
        return sendResult;
    }

    private ONSClientException checkProducerException(String topic, String msgId, Throwable e) {
        if (e instanceof MQClientException) {
            if (e.getCause() != null) {
                if (e.getCause() instanceof RemotingConnectException) {
                    return new ONSClientException(
                        FAQ.errorMessage(String.format("Connect broker failed, Topic=%s, msgId=%s", topic, msgId), FAQ.CONNECT_BROKER_FAILED));
                } else if (e.getCause() instanceof RemotingTimeoutException) {
                    return new ONSClientException(FAQ.errorMessage(String.format("Send message to broker timeout, %dms, Topic=%s, msgId=%s",
                        this.defaultMQProducer.getSendMsgTimeout(), topic, msgId), FAQ.SEND_MSG_TO_BROKER_TIMEOUT));
                } else if (e.getCause() instanceof MQBrokerException) {
                    MQBrokerException excep = (MQBrokerException) e.getCause();
                    return new ONSClientException(FAQ.errorMessage(
                        String.format("Receive a broker exception, Topic=%s, msgId=%s, %s", topic, msgId, excep.getErrorMessage()),
                        FAQ.BROKER_RESPONSE_EXCEPTION));
                }
            } else {
                MQClientException excep = (MQClientException) e;
                if (-1 == excep.getResponseCode()) {
                    return new ONSClientException(
                        FAQ.errorMessage(String.format("Topic does not exist, Topic=%s, msgId=%s", topic, msgId), FAQ.TOPIC_ROUTE_NOT_EXIST));
                } else if (ResponseCode.MESSAGE_ILLEGAL == excep.getResponseCode()) {
                    return new ONSClientException(
                        FAQ.errorMessage(String.format("ONS Client check message exception, Topic=%s, msgId=%s", topic, msgId),
                            FAQ.CLIENT_CHECK_MSG_EXCEPTION));
                }
            }
        }

        return new ONSClientException("defaultMQProducer send exception", e);
    }
}
