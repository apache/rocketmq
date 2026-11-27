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

package org.apache.rocketmq.proxy.service.sysmessage;

import com.alibaba.fastjson2.JSON;
import io.netty.util.AttributeKey;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.broker.client.ClientChannelInfo;
import org.apache.rocketmq.broker.client.ConsumerManager;
import org.apache.rocketmq.broker.client.ConsumerIdsChangeListener;
import org.apache.rocketmq.client.impl.mqclient.MQClientAPIFactory;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.proxy.config.InitConfigTest;
import org.apache.rocketmq.proxy.processor.channel.ChannelProtocolType;
import org.apache.rocketmq.proxy.processor.channel.RemoteChannel;
import org.apache.rocketmq.proxy.remoting.channel.RemotingChannel;
import org.apache.rocketmq.proxy.service.admin.AdminService;
import org.apache.rocketmq.proxy.service.client.ClusterConsumerManager;
import org.apache.rocketmq.proxy.service.route.TopicRouteService;
import org.apache.rocketmq.remoting.protocol.LanguageCode;
import org.apache.rocketmq.remoting.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.remoting.protocol.heartbeat.MessageModel;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class HeartbeatSyncerExtInfoTest extends InitConfigTest {
    private static final AttributeKey<String> TENANT = AttributeKey.valueOf("heartbeat-test-tenant");
    private ConsumerManager consumerManager;
    private RecordingSyncer syncer;

    @Before
    public void setUp() {
        consumerManager = mock(ConsumerManager.class);
        syncer = new RecordingSyncer(consumerManager);
    }

    @After
    public void tearDown() {
        syncer.threadPoolExecutor.shutdownNow();
    }

    @Test
    public void testExtInfoIsAppliedBeforeRegistrationToReusedChannel() throws Exception {
        syncer.setHeartbeatSyncerDataHandlers(null, (data, channel) -> channel.attr(TENANT).set(
            data.getExtInfo() == null ? null : data.getExtInfo().get("tenant")));
        AtomicInteger registered = new AtomicInteger();
        doAnswer(invocation -> {
            ClientChannelInfo info = invocation.getArgument(1);
            assertEquals(registered.getAndIncrement() < 2 ? "tenant-a" : null, info.getChannel().attr(TENANT).get());
            return true;
        }).when(consumerManager).registerConsumer(anyString(), any(), any(), any(), any(), any(), anyBoolean());

        consume(heartbeat(HeartbeatType.REGISTER, Collections.singletonMap("tenant", "tenant-a")));
        RemoteChannel channel = syncer.remoteChannelMap.values().iterator().next();
        consume(heartbeat(HeartbeatType.REGISTER, Collections.singletonMap("tenant", "tenant-a")));
        assertSame(channel, syncer.remoteChannelMap.values().iterator().next());
        consume(heartbeat(HeartbeatType.REGISTER, null));
        assertNull(channel.attr(TENANT).get());
        assertEquals("opaque-settings", channel.getChannelExtendAttribute());
        assertEquals(3, registered.get());
    }

    @Test
    public void testExtInfoIsAppliedBeforeUnregister() throws Exception {
        syncer.setHeartbeatSyncerDataHandlers(null, (data, channel) -> channel.attr(TENANT).set(data.getExtInfo().get("tenant")));
        consume(heartbeat(HeartbeatType.REGISTER, Collections.singletonMap("tenant", "tenant-a")));
        RemoteChannel channel = syncer.remoteChannelMap.values().iterator().next();
        doAnswer(invocation -> {
            ClientChannelInfo info = invocation.getArgument(1);
            assertSame(channel, info.getChannel());
            assertEquals("tenant-b", info.getChannel().attr(TENANT).get());
            return null;
        }).when(consumerManager).unregisterConsumer(anyString(), any(), anyBoolean());
        consume(heartbeat(HeartbeatType.UNREGISTER, Collections.singletonMap("tenant", "tenant-b")));
        verify(consumerManager).unregisterConsumer(anyString(), any(), anyBoolean());
    }

    @Test
    public void testReceiverFailureDoesNotBlockRegistrationOrUnregister() throws Exception {
        syncer.setHeartbeatSyncerDataHandlers(null, (data, channel) -> { throw new IllegalStateException("extension unavailable"); });
        consume(heartbeat(HeartbeatType.REGISTER, Collections.emptyMap()));
        consume(heartbeat(HeartbeatType.UNREGISTER, Collections.emptyMap()));
        verify(consumerManager).registerConsumer(anyString(), any(), any(), any(), any(), any(), anyBoolean());
        verify(consumerManager).unregisterConsumer(anyString(), any(), anyBoolean());
    }

    @Test
    public void testOwnBroadcastDoesNotInvokeReceiver() throws Exception {
        AtomicInteger received = new AtomicInteger();
        syncer.setHeartbeatSyncerDataHandlers(null, (data, channel) -> received.incrementAndGet());
        HeartbeatSyncerData data = heartbeat(HeartbeatType.REGISTER, Collections.emptyMap());
        data.setLocalProxyId(syncer.localProxyId);
        consume(data);
        assertEquals(0, received.get());
        verify(consumerManager, never()).registerConsumer(anyString(), any(), any(), any(), any(), any(), anyBoolean());
    }

    @Test
    public void testSenderEnrichesRegisterAndUnregister() throws Exception {
        syncer.setHeartbeatSyncerDataHandlers(data -> data.setExtInfo(Collections.singletonMap("tenant", "tenant-a")), null);
        ClientChannelInfo info = localClient();
        syncer.onConsumerRegister("group", info, ConsumeType.CONSUME_PASSIVELY, MessageModel.CLUSTERING,
            ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET, Collections.emptySet());
        HeartbeatSyncerData registered = syncer.sent.poll(3, TimeUnit.SECONDS);
        assertNotNull(registered);
        assertEquals("tenant-a", registered.getExtInfo().get("tenant"));
        assertEquals(HeartbeatType.REGISTER, registered.getHeartbeatType());
        syncer.onConsumerUnRegister("group", info);
        HeartbeatSyncerData unregistered = syncer.sent.poll(3, TimeUnit.SECONDS);
        assertNotNull(unregistered);
        assertEquals("tenant-a", unregistered.getExtInfo().get("tenant"));
        assertEquals(HeartbeatType.UNREGISTER, unregistered.getHeartbeatType());
    }

    @Test
    public void testSenderFailureDoesNotBlockBroadcast() throws Exception {
        syncer.setHeartbeatSyncerDataHandlers(data -> { throw new IllegalStateException("extension unavailable"); }, null);
        syncer.onConsumerUnRegister("group", localClient());
        HeartbeatSyncerData sent = syncer.sent.poll(3, TimeUnit.SECONDS);
        assertNotNull(sent);
        assertEquals("group", sent.getGroup());
    }

    @Test
    public void testManagerDelegatesReceiverConfiguration() throws Exception {
        ClusterConsumerManager manager = new ClusterConsumerManager(mock(TopicRouteService.class), mock(AdminService.class),
            mock(MQClientAPIFactory.class), mock(ConsumerIdsChangeListener.class), 60000, null);
        java.lang.reflect.Field field = ClusterConsumerManager.class.getDeclaredField("heartbeatSyncer");
        field.setAccessible(true);
        HeartbeatSyncer managerSyncer = (HeartbeatSyncer) field.get(manager);
        try {
            manager.setHeartbeatSyncerDataHandlers(null, (data, channel) -> channel.attr(TENANT).set("tenant-a"));
            MessageExt message = new MessageExt();
            message.setBody(JSON.toJSONBytes(heartbeat(HeartbeatType.REGISTER, Collections.emptyMap())));
            managerSyncer.consumeMessage(Collections.singletonList(message), null);
            assertEquals("tenant-a", manager.findChannel("group", "client").getChannel().attr(TENANT).get());
        } finally {
            managerSyncer.threadPoolExecutor.shutdownNow();
        }
    }

    private ClientChannelInfo localClient() {
        RemotingChannel channel = mock(RemotingChannel.class);
        when(channel.toRemoteChannel()).thenReturn(new RemoteChannel("remote-proxy", "remote", "local",
            ChannelProtocolType.REMOTING, "opaque-settings"));
        return new ClientChannelInfo(channel, "client", LanguageCode.JAVA, 1);
    }

    private HeartbeatSyncerData heartbeat(HeartbeatType type, Map<String, String> extInfo) {
        RemoteChannel channel = new RemoteChannel("remote-proxy", "remote", "local", ChannelProtocolType.REMOTING,
            "opaque-settings");
        HeartbeatSyncerData data = new HeartbeatSyncerData(type, "client", LanguageCode.JAVA, 1, "group",
            ConsumeType.CONSUME_PASSIVELY, MessageModel.CLUSTERING, ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET,
            "remote-proxy", channel.encode());
        data.setSubscriptionDataSet(Collections.emptySet());
        data.setExtInfo(extInfo);
        return data;
    }

    private void consume(HeartbeatSyncerData data) {
        MessageExt message = new MessageExt();
        message.setBody(JSON.toJSONBytes(data));
        syncer.consumeMessage(Collections.singletonList(message), null);
    }

    private static class RecordingSyncer extends HeartbeatSyncer {
        private final BlockingQueue<HeartbeatSyncerData> sent = new LinkedBlockingQueue<>();

        RecordingSyncer(ConsumerManager consumerManager) {
            super(mock(TopicRouteService.class), mock(AdminService.class), consumerManager,
                mock(MQClientAPIFactory.class), null);
        }

        @Override
        protected void sendSystemMessage(Object data) {
            sent.add((HeartbeatSyncerData) data);
        }
    }
}
