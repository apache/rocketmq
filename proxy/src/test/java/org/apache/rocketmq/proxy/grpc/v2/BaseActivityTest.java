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

package org.apache.rocketmq.proxy.grpc.v2;

import io.grpc.Metadata;
import java.time.Duration;
import java.util.Random;
import java.util.UUID;
import org.apache.rocketmq.common.protocol.header.ExtraInfoUtil;
import org.apache.rocketmq.proxy.common.ContextVariable;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.config.InitConfigAndLoggerTest;
import org.apache.rocketmq.proxy.grpc.interceptor.InterceptorConstants;
import org.apache.rocketmq.proxy.grpc.v2.channel.GrpcChannelManager;
import org.apache.rocketmq.proxy.grpc.v2.common.GrpcClientSettingsManager;
import org.apache.rocketmq.proxy.processor.MessagingProcessor;
import org.apache.rocketmq.proxy.processor.ReceiptHandleProcessor;
import org.apache.rocketmq.proxy.service.metadata.MetadataService;
import org.apache.rocketmq.proxy.service.relay.ProxyRelayService;
import org.junit.Ignore;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Ignore
@RunWith(MockitoJUnitRunner.Silent.class)
public class BaseActivityTest extends InitConfigAndLoggerTest {
    protected static final Random RANDOM = new Random();
    protected MessagingProcessor messagingProcessor;
    protected GrpcClientSettingsManager grpcClientSettingsManager;
    protected GrpcChannelManager grpcChannelManager;
    protected ProxyRelayService proxyRelayService;
    protected ReceiptHandleProcessor receiptHandleProcessor;
    protected MetadataService metadataService;

    protected static final String REMOTE_ADDR = "192.168.0.1:8080";
    protected static final String LOCAL_ADDR = "127.0.0.1:8080";
    protected Metadata metadata = new Metadata();

    protected static final String CLIENT_ID = "client-id" + UUID.randomUUID();
    protected static final String JAVA = "JAVA";

    public void before() throws Throwable {
        super.before();
        messagingProcessor = mock(MessagingProcessor.class);
        grpcClientSettingsManager = mock(GrpcClientSettingsManager.class);
        proxyRelayService = mock(ProxyRelayService.class);
        receiptHandleProcessor = mock(ReceiptHandleProcessor.class);
        metadataService = mock(MetadataService.class);

        metadata.put(InterceptorConstants.CLIENT_ID, CLIENT_ID);
        metadata.put(InterceptorConstants.LANGUAGE, JAVA);
        metadata.put(InterceptorConstants.REMOTE_ADDRESS, REMOTE_ADDR);
        metadata.put(InterceptorConstants.LOCAL_ADDRESS, LOCAL_ADDR);
        when(messagingProcessor.getProxyRelayService()).thenReturn(proxyRelayService);
        when(messagingProcessor.getMetadataService()).thenReturn(metadataService);
        grpcChannelManager = new GrpcChannelManager(messagingProcessor.getProxyRelayService());
    }

    protected ProxyContext createContext() {
        return ProxyContext.create()
            .withVal(ContextVariable.CLIENT_ID, CLIENT_ID)
            .withVal(ContextVariable.LANGUAGE, JAVA)
            .withVal(ContextVariable.REMOTE_ADDRESS, REMOTE_ADDR)
            .withVal(ContextVariable.LOCAL_ADDRESS, LOCAL_ADDR)
            .withVal(ContextVariable.REMAINING_MS, Duration.ofSeconds(10).toMillis());
    }

    protected static String buildReceiptHandle(String topic, long popTime, long invisibleTime) {
        return ExtraInfoUtil.buildExtraInfo(
            RANDOM.nextInt(Integer.MAX_VALUE),
            popTime,
            invisibleTime,
            0,
            topic,
            "brokerName",
            RANDOM.nextInt(8),
            RANDOM.nextInt(Integer.MAX_VALUE)
        );
    }
}
