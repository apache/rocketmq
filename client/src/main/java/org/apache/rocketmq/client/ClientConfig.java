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
package org.apache.rocketmq.client;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.utils.NameServerAddressUtils;
import org.apache.rocketmq.common.utils.NetworkUtil;
import org.apache.rocketmq.remoting.netty.TlsSystemConfig;
import org.apache.rocketmq.remoting.protocol.LanguageCode;
import org.apache.rocketmq.remoting.protocol.NamespaceUtil;
import org.apache.rocketmq.remoting.protocol.RequestType;

/**
 * Client Common configuration
 */
public class ClientConfig {
    public static final String SEND_MESSAGE_WITH_VIP_CHANNEL_PROPERTY = "com.rocketmq.sendMessageWithVIPChannel";
    public static final String SOCKS_PROXY_CONFIG = "com.rocketmq.socks.proxy.config";
    public static final String DECODE_READ_BODY = "com.rocketmq.read.body";
    public static final String DECODE_DECOMPRESS_BODY = "com.rocketmq.decompress.body";
    public static final String SEND_LATENCY_ENABLE = "com.rocketmq.sendLatencyEnable";
    public static final String START_DETECTOR_ENABLE = "com.rocketmq.startDetectorEnable";
    public static final String HEART_BEAT_V2 = "com.rocketmq.heartbeat.v2";
    private String namesrvAddr = NameServerAddressUtils.getNameServerAddresses();
    private String clientIP = NetworkUtil.getLocalAddress();
    private String instanceName = System.getProperty("rocketmq.client.name", "DEFAULT");
    private int clientCallbackExecutorThreads = Runtime.getRuntime().availableProcessors();
    @Deprecated
    protected String namespace;
    private boolean namespaceInitialized = false;
    protected String namespaceV2;
    protected AccessChannel accessChannel = AccessChannel.LOCAL;

    /**
     * Pulling topic information interval from the named server
     */
    private int pollNameServerInterval = 1000 * 30;
    /**
     * Heartbeat interval in microseconds with message broker
     */
    private int heartbeatBrokerInterval = 1000 * 30;
    /**
     * Offset persistent interval for consumer
     */
    private int persistConsumerOffsetInterval = 1000 * 5;
    private long pullTimeDelayMillsWhenException = 1000;

    private int traceMsgBatchNum = 10;
    private boolean unitMode = false;
    private String unitName;
    private boolean decodeReadBody = Boolean.parseBoolean(System.getProperty(DECODE_READ_BODY, "true"));
    private boolean decodeDecompressBody = Boolean.parseBoolean(System.getProperty(DECODE_DECOMPRESS_BODY, "true"));
    private boolean vipChannelEnabled = Boolean.parseBoolean(System.getProperty(SEND_MESSAGE_WITH_VIP_CHANNEL_PROPERTY, "false"));
    private boolean useHeartbeatV2 = Boolean.parseBoolean(System.getProperty(HEART_BEAT_V2, "false"));

    private boolean useTLS = TlsSystemConfig.tlsEnable;

    private String socksProxyConfig = System.getProperty(SOCKS_PROXY_CONFIG, "{}");

    private int mqClientApiTimeout = 3 * 1000;
    private int detectTimeout = 200;
    private int detectInterval = 2 * 1000;

    private LanguageCode language = LanguageCode.JAVA;

    /**
     * Enable stream request type will inject a RPCHook to add corresponding request type to remoting layer.
     * And it will also generate a different client id to prevent unexpected reuses of MQClientInstance.
     */
    protected boolean enableStreamRequestType = false;

    /**
     * Enable the fault tolerance mechanism of the client sending process.
     * DO NOT OPEN when ORDER messages are required.
     * Turning on will interfere with the queue selection functionality,
     * possibly conflicting with the order message.
     */
    private boolean sendLatencyEnable = Boolean.parseBoolean(System.getProperty(SEND_LATENCY_ENABLE, "false"));
    private boolean startDetectorEnable = Boolean.parseBoolean(System.getProperty(START_DETECTOR_ENABLE, "false"));

    private boolean enableHeartbeatChannelEventListener = true;

    /**
     * The switch for message trace
     */
    protected boolean enableTrace = false;

    /**
     * The name value of message trace topic. If not set, the default trace topic name will be used.
     */
    protected String traceTopic;

    protected int maxPageSizeInGetMetadata = 2000;

    public String buildMQClientId() {
        StringBuilder sb = new StringBuilder();
        sb.append(this.getClientIP());

        sb.append("@");
        sb.append(this.getInstanceName());
        if (!UtilAll.isBlank(this.unitName)) {
            sb.append("@");
            sb.append(this.unitName);
        }

        if (enableStreamRequestType) {
            sb.append("@");
            sb.append(RequestType.STREAM);
        }

        return sb.toString();
    }

    public int getTraceMsgBatchNum() {
        return traceMsgBatchNum;
    }

    public void setTraceMsgBatchNum(int traceMsgBatchNum) {
        this.traceMsgBatchNum = traceMsgBatchNum;
    }

    public String getClientIP() {
        return clientIP;
    }

    public void setClientIP(String clientIP) {
        this.clientIP = clientIP;
    }

    public String getInstanceName() {
        return instanceName;
    }

    public void setInstanceName(String instanceName) {
        this.instanceName = instanceName;
    }

    public void changeInstanceNameToPID() {
        if (this.instanceName.equals("DEFAULT")) {
            this.instanceName = UtilAll.getPid() + "#" + System.nanoTime();
        }
    }

    @Deprecated
    public String withNamespace(String resource) {
        return NamespaceUtil.wrapNamespace(this.getNamespace(), resource);
    }

    @Deprecated
    public Set<String> withNamespace(Set<String> resourceSet) {
        Set<String> resourceWithNamespace = new HashSet<>();
        for (String resource : resourceSet) {
            resourceWithNamespace.add(withNamespace(resource));
        }
        return resourceWithNamespace;
    }

    @Deprecated
    public String withoutNamespace(String resource) {
        return NamespaceUtil.withoutNamespace(resource, this.getNamespace());
    }

    @Deprecated
    public Set<String> withoutNamespace(Set<String> resourceSet) {
        Set<String> resourceWithoutNamespace = new HashSet<>();
        for (String resource : resourceSet) {
            resourceWithoutNamespace.add(withoutNamespace(resource));
        }
        return resourceWithoutNamespace;
    }

    @Deprecated
    public MessageQueue queueWithNamespace(MessageQueue queue) {
        if (StringUtils.isEmpty(this.getNamespace())) {
            return queue;
        }
        return new MessageQueue(withNamespace(queue.getTopic()), queue.getBrokerName(), queue.getQueueId());
    }

    @Deprecated
    public Collection<MessageQueue> queuesWithNamespace(Collection<MessageQueue> queues) {
        if (StringUtils.isEmpty(this.getNamespace())) {
            return queues;
        }
        Iterator<MessageQueue> iter = queues.iterator();
        while (iter.hasNext()) {
            MessageQueue queue = iter.next();
            queue.setTopic(withNamespace(queue.getTopic()));
        }
        return queues;
    }

    public void resetClientConfig(final ClientConfig cc) {
        this.namesrvAddr = cc.namesrvAddr;
        this.clientIP = cc.clientIP;
        this.instanceName = cc.instanceName;
        this.clientCallbackExecutorThreads = cc.clientCallbackExecutorThreads;
        this.pollNameServerInterval = cc.pollNameServerInterval;
        this.heartbeatBrokerInterval = cc.heartbeatBrokerInterval;
        this.persistConsumerOffsetInterval = cc.persistConsumerOffsetInterval;
        this.pullTimeDelayMillsWhenException = cc.pullTimeDelayMillsWhenException;
        this.unitMode = cc.unitMode;
        this.unitName = cc.unitName;
        this.vipChannelEnabled = cc.vipChannelEnabled;
        this.useTLS = cc.useTLS;
        this.socksProxyConfig = cc.socksProxyConfig;
        this.namespace = cc.namespace;
        this.language = cc.language;
        this.mqClientApiTimeout = cc.mqClientApiTimeout;
        this.decodeReadBody = cc.decodeReadBody;
        this.decodeDecompressBody = cc.decodeDecompressBody;
        this.enableStreamRequestType = cc.enableStreamRequestType;
        this.useHeartbeatV2 = cc.useHeartbeatV2;
        this.startDetectorEnable = cc.startDetectorEnable;
        this.sendLatencyEnable = cc.sendLatencyEnable;
        this.enableHeartbeatChannelEventListener = cc.enableHeartbeatChannelEventListener;
        this.detectInterval = cc.detectInterval;
        this.detectTimeout = cc.detectTimeout;
        this.namespaceV2 = cc.namespaceV2;
        this.enableTrace = cc.enableTrace;
        this.traceTopic = cc.traceTopic;
    }

    public ClientConfig cloneClientConfig() {
        ClientConfig cc = new ClientConfig();
        cc.namesrvAddr = namesrvAddr;
        cc.clientIP = clientIP;
        cc.instanceName = instanceName;
        cc.clientCallbackExecutorThreads = clientCallbackExecutorThreads;
        cc.pollNameServerInterval = pollNameServerInterval;
        cc.heartbeatBrokerInterval = heartbeatBrokerInterval;
        cc.persistConsumerOffsetInterval = persistConsumerOffsetInterval;
        cc.pullTimeDelayMillsWhenException = pullTimeDelayMillsWhenException;
        cc.unitMode = unitMode;
        cc.unitName = unitName;
        cc.vipChannelEnabled = vipChannelEnabled;
        cc.useTLS = useTLS;
        cc.socksProxyConfig = socksProxyConfig;
        cc.namespace = namespace;
        cc.language = language;
        cc.mqClientApiTimeout = mqClientApiTimeout;
        cc.decodeReadBody = decodeReadBody;
        cc.decodeDecompressBody = decodeDecompressBody;
        cc.enableStreamRequestType = enableStreamRequestType;
        cc.useHeartbeatV2 = useHeartbeatV2;
        cc.startDetectorEnable = startDetectorEnable;
        cc.enableHeartbeatChannelEventListener = enableHeartbeatChannelEventListener;
        cc.sendLatencyEnable = sendLatencyEnable;
        cc.detectInterval = detectInterval;
        cc.detectTimeout = detectTimeout;
        cc.namespaceV2 = namespaceV2;
        cc.enableTrace = enableTrace;
        cc.traceTopic = traceTopic;
        return cc;
    }

    public String getNamesrvAddr() {
        if (StringUtils.isNotEmpty(namesrvAddr) && NameServerAddressUtils.NAMESRV_ENDPOINT_PATTERN.matcher(namesrvAddr.trim()).matches()) {
            return NameServerAddressUtils.getNameSrvAddrFromNamesrvEndpoint(namesrvAddr);
        }
        return namesrvAddr;
    }

    /**
     * Domain name mode access way does not support the delimiter(;), and only one domain name can be set.
     *
     * @param namesrvAddr name server address
     */
    public void setNamesrvAddr(String namesrvAddr) {
        this.namesrvAddr = namesrvAddr;
        this.namespaceInitialized = false;
    }

    public int getClientCallbackExecutorThreads() {
        return clientCallbackExecutorThreads;
    }

    public void setClientCallbackExecutorThreads(int clientCallbackExecutorThreads) {
        this.clientCallbackExecutorThreads = clientCallbackExecutorThreads;
    }

    public int getPollNameServerInterval() {
        return pollNameServerInterval;
    }

    public void setPollNameServerInterval(int pollNameServerInterval) {
        this.pollNameServerInterval = pollNameServerInterval;
    }

    public int getHeartbeatBrokerInterval() {
        return heartbeatBrokerInterval;
    }

    public void setHeartbeatBrokerInterval(int heartbeatBrokerInterval) {
        this.heartbeatBrokerInterval = heartbeatBrokerInterval;
    }

    public int getPersistConsumerOffsetInterval() {
        return persistConsumerOffsetInterval;
    }

    public void setPersistConsumerOffsetInterval(int persistConsumerOffsetInterval) {
        this.persistConsumerOffsetInterval = persistConsumerOffsetInterval;
    }

    public long getPullTimeDelayMillsWhenException() {
        return pullTimeDelayMillsWhenException;
    }

    public void setPullTimeDelayMillsWhenException(long pullTimeDelayMillsWhenException) {
        this.pullTimeDelayMillsWhenException = pullTimeDelayMillsWhenException;
    }

    public String getUnitName() {
        return unitName;
    }

    public void setUnitName(String unitName) {
        this.unitName = unitName;
    }

    public boolean isUnitMode() {
        return unitMode;
    }

    public void setUnitMode(boolean unitMode) {
        this.unitMode = unitMode;
    }

    public boolean isVipChannelEnabled() {
        return vipChannelEnabled;
    }

    public void setVipChannelEnabled(final boolean vipChannelEnabled) {
        this.vipChannelEnabled = vipChannelEnabled;
    }

    public boolean isUseTLS() {
        return useTLS;
    }

    public void setUseTLS(boolean useTLS) {
        this.useTLS = useTLS;
    }

    public String getSocksProxyConfig() {
        return socksProxyConfig;
    }

    public void setSocksProxyConfig(String socksProxyConfig) {
        this.socksProxyConfig = socksProxyConfig;
    }

    public LanguageCode getLanguage() {
        return language;
    }

    public void setLanguage(LanguageCode language) {
        this.language = language;
    }

    public boolean isDecodeReadBody() {
        return decodeReadBody;
    }

    public void setDecodeReadBody(boolean decodeReadBody) {
        this.decodeReadBody = decodeReadBody;
    }

    public boolean isDecodeDecompressBody() {
        return decodeDecompressBody;
    }

    public void setDecodeDecompressBody(boolean decodeDecompressBody) {
        this.decodeDecompressBody = decodeDecompressBody;
    }

    @Deprecated
    public String getNamespace() {
        if (namespaceInitialized) {
            return namespace;
        }

        if (StringUtils.isNotEmpty(namespace)) {
            return namespace;
        }

        if (StringUtils.isNotEmpty(this.namesrvAddr)) {
            if (NameServerAddressUtils.validateInstanceEndpoint(namesrvAddr)) {
                namespace = NameServerAddressUtils.parseInstanceIdFromEndpoint(namesrvAddr);
            }
        }
        namespaceInitialized = true;
        return namespace;
    }

    @Deprecated
    public void setNamespace(String namespace) {
        this.namespace = namespace;
        this.namespaceInitialized = true;
    }

    public String getNamespaceV2() {
        return namespaceV2;
    }

    public void setNamespaceV2(String namespaceV2) {
        this.namespaceV2 = namespaceV2;
        this.namespace = namespaceV2;
    }

    public AccessChannel getAccessChannel() {
        return this.accessChannel;
    }

    public void setAccessChannel(AccessChannel accessChannel) {
        this.accessChannel = accessChannel;
    }

    public int getMqClientApiTimeout() {
        return mqClientApiTimeout;
    }

    public void setMqClientApiTimeout(int mqClientApiTimeout) {
        this.mqClientApiTimeout = mqClientApiTimeout;
    }

    public boolean isEnableStreamRequestType() {
        return enableStreamRequestType;
    }

    public void setEnableStreamRequestType(boolean enableStreamRequestType) {
        this.enableStreamRequestType = enableStreamRequestType;
    }

    public boolean isSendLatencyEnable() {
        return sendLatencyEnable;
    }

    public void setSendLatencyEnable(boolean sendLatencyEnable) {
        this.sendLatencyEnable = sendLatencyEnable;
    }

    public boolean isStartDetectorEnable() {
        return startDetectorEnable;
    }

    public void setStartDetectorEnable(boolean startDetectorEnable) {
        this.startDetectorEnable = startDetectorEnable;
    }

    public boolean isEnableHeartbeatChannelEventListener() {
        return enableHeartbeatChannelEventListener;
    }

    public void setEnableHeartbeatChannelEventListener(boolean enableHeartbeatChannelEventListener) {
        this.enableHeartbeatChannelEventListener = enableHeartbeatChannelEventListener;
    }

    public int getDetectTimeout() {
        return this.detectTimeout;
    }

    public void setDetectTimeout(int detectTimeout) {
        this.detectTimeout = detectTimeout;
    }

    public int getDetectInterval() {
        return this.detectInterval;
    }

    public void setDetectInterval(int detectInterval) {
        this.detectInterval = detectInterval;
    }

    public boolean isUseHeartbeatV2() {
        return useHeartbeatV2;
    }

    public void setUseHeartbeatV2(boolean useHeartbeatV2) {
        this.useHeartbeatV2 = useHeartbeatV2;
    }

    public boolean isEnableTrace() {
        return enableTrace;
    }

    public void setEnableTrace(boolean enableTrace) {
        this.enableTrace = enableTrace;
    }

    public String getTraceTopic() {
        return traceTopic;
    }

    public void setTraceTopic(String traceTopic) {
        this.traceTopic = traceTopic;
    }

    public int getMaxPageSizeInGetMetadata() {
        return maxPageSizeInGetMetadata;
    }

    public void setMaxPageSizeInGetMetadata(int maxPageSizeInGetMetadata) {
        this.maxPageSizeInGetMetadata = maxPageSizeInGetMetadata;
    }

    @Override
    public String toString() {
        return "ClientConfig{" +
            "namesrvAddr='" + namesrvAddr + '\'' +
            ", clientIP='" + clientIP + '\'' +
            ", instanceName='" + instanceName + '\'' +
            ", clientCallbackExecutorThreads=" + clientCallbackExecutorThreads +
            ", namespace='" + namespace + '\'' +
            ", namespaceInitialized=" + namespaceInitialized +
            ", namespaceV2='" + namespaceV2 + '\'' +
            ", accessChannel=" + accessChannel +
            ", pollNameServerInterval=" + pollNameServerInterval +
            ", heartbeatBrokerInterval=" + heartbeatBrokerInterval +
            ", persistConsumerOffsetInterval=" + persistConsumerOffsetInterval +
            ", pullTimeDelayMillsWhenException=" + pullTimeDelayMillsWhenException +
            ", unitMode=" + unitMode +
            ", unitName='" + unitName + '\'' +
            ", decodeReadBody=" + decodeReadBody +
            ", decodeDecompressBody=" + decodeDecompressBody +
            ", vipChannelEnabled=" + vipChannelEnabled +
            ", useHeartbeatV2=" + useHeartbeatV2 +
            ", useTLS=" + useTLS +
            ", socksProxyConfig='" + socksProxyConfig + '\'' +
            ", mqClientApiTimeout=" + mqClientApiTimeout +
            ", detectTimeout=" + detectTimeout +
            ", detectInterval=" + detectInterval +
            ", language=" + language +
            ", enableStreamRequestType=" + enableStreamRequestType +
            ", sendLatencyEnable=" + sendLatencyEnable +
            ", startDetectorEnable=" + startDetectorEnable +
            ", enableHeartbeatChannelEventListener=" + enableHeartbeatChannelEventListener +
            ", enableTrace=" + enableTrace +
            ", traceTopic='" + traceTopic + '\'' +
            '}';
    }
}
