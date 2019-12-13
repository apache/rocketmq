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
import io.openmessaging.api.order.OrderProducer;
import java.util.List;
import java.util.Properties;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.acl.common.AclClientRPCHook;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.ons.api.PropertyKeyConst;
import org.apache.rocketmq.ons.api.exception.ONSClientException;
import org.apache.rocketmq.ons.api.impl.util.ClientLoggerUtil;
import org.apache.rocketmq.ons.open.trace.core.common.OnsTraceConstants;
import org.apache.rocketmq.ons.open.trace.core.common.OnsTraceDispatcherType;
import org.apache.rocketmq.ons.open.trace.core.dispatch.impl.AsyncArrayDispatcher;
import org.apache.rocketmq.ons.open.trace.core.hook.OnsClientSendMessageHookImpl;
import org.apache.rocketmq.remoting.protocol.LanguageCode;

public class OrderProducerImpl extends ONSClientAbstract implements OrderProducer {
    private final static InternalLogger LOGGER = ClientLoggerUtil.getClientLogger();
    private final DefaultMQProducer defaultMQProducer;

    public OrderProducerImpl(final Properties properties) {
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

        String sendMsgTimeoutMillis = properties.getProperty(PropertyKeyConst.SendMsgTimeoutMillis, "3000");
        this.defaultMQProducer.setSendMsgTimeout(Integer.parseInt(sendMsgTimeoutMillis));

//        boolean addExtendUniqInfo = Boolean.parseBoolean(properties.getProperty(PropertyKeyConst.EXACTLYONCE_DELIVERY, "false"));
//        this.defaultMQProducer.setAddExtendUniqInfo(addExtendUniqInfo);

        if (properties.containsKey(PropertyKeyConst.LANGUAGE_IDENTIFIER)) {
            int language = Integer.valueOf(properties.get(PropertyKeyConst.LANGUAGE_IDENTIFIER).toString());
            byte languageByte = (byte) language;
            this.defaultMQProducer.setLanguage(LanguageCode.valueOf(languageByte));
        }
        String instanceName = properties.getProperty(PropertyKeyConst.InstanceName, this.buildIntanceName());
        this.defaultMQProducer.setInstanceName(instanceName);
        this.defaultMQProducer.setNamesrvAddr(this.getNameServerAddr());
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
                dispatcher.setHostProducer(defaultMQProducer.getDefaultMQProducerImpl());
                traceDispatcher = dispatcher;
                this.defaultMQProducer.getDefaultMQProducerImpl().registerSendMessageHook(
                    new OnsClientSendMessageHookImpl(traceDispatcher));
            } catch (Throwable e) {
                LOGGER.error("system mqtrace hook init failed ,maybe can't send msg trace data", e);
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
            if (started.compareAndSet(false, true)) {
                this.defaultMQProducer.start();
                super.start();
            }
        } catch (Exception e) {
            throw new ONSClientException(e.getMessage());
        }
    }

    @Override
    public void shutdown() {
        if (started.compareAndSet(true, false)) {
            this.defaultMQProducer.shutdown();
        }
        super.shutdown();
    }

    @Override
    public SendResult send(final Message message, final String shardingKey) {
        if (UtilAll.isBlank(shardingKey)) {
            throw new ONSClientException("\'shardingKey\' is blank.");
        }
        message.setShardingKey(shardingKey);
        this.checkONSProducerServiceState(this.defaultMQProducer.getDefaultMQProducerImpl());
        final org.apache.rocketmq.common.message.Message msgRMQ = ONSUtil.msgConvert(message);
        try {
            org.apache.rocketmq.client.producer.SendResult sendResultRMQ =
                this.defaultMQProducer.send(msgRMQ, new org.apache.rocketmq.client.producer.MessageQueueSelector() {
                    @Override
                    public MessageQueue select(List<MessageQueue> mqs, org.apache.rocketmq.common.message.Message msg,
                        Object shardingKey) {
                        int select = Math.abs(shardingKey.hashCode());
                        if (select < 0) {
                            select = 0;
                        }
                        return mqs.get(select % mqs.size());
                    }
                }, shardingKey);
            message.setMsgID(sendResultRMQ.getMsgId());
            SendResult sendResult = new SendResult();
            sendResult.setTopic(message.getTopic());
            sendResult.setMessageId(sendResultRMQ.getMsgId());
            return sendResult;
        } catch (Exception e) {
            throw new ONSClientException("defaultMQProducer send order exception", e);
        }
    }
}
