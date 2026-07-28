/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.proxy.grpc.v2.common;

import apache.rocketmq.v2.ClientType;
import apache.rocketmq.v2.CustomizedBackoff;
import apache.rocketmq.v2.ExponentialBackoff;
import apache.rocketmq.v2.Publishing;
import apache.rocketmq.v2.Resource;
import apache.rocketmq.v2.RetryPolicy;
import apache.rocketmq.v2.Settings;
import apache.rocketmq.v2.Subscription;
import apache.rocketmq.v2.SubscriptionEntry;
import com.google.protobuf.util.Durations;
import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.common.lite.LiteSubscriptionDTO;
import org.apache.rocketmq.proxy.common.ContextVariable;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.grpc.v2.BaseActivityTest;
import org.apache.rocketmq.remoting.protocol.subscription.CustomizedRetryPolicy;
import org.apache.rocketmq.remoting.protocol.subscription.ExponentialRetryPolicy;
import org.apache.rocketmq.remoting.protocol.subscription.GroupRetryPolicyType;
import org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class GrpcClientSettingsManagerTest extends BaseActivityTest {

    private final ProxyContext ctx = ProxyContext.create();
    private final String clientId = "testClientId";
    private final String generationClientId = "generationClientId";
    private final String sameGenerationClientId = "sameGenerationClientId";
    private final String legacyClientId = "legacyClientId";
    private final String invalidGenerationClientId = "invalidGenerationClientId";
    private final String unconditionalRemovalClientId = "unconditionalRemovalClientId";
    private final String cleanerProducerClientId = "cleanerProducerClientId";
    private final String cleanerConsumerClientId = "cleanerConsumerClientId";
    private final String instanceIsolationClientId = "instanceIsolationClientId";
    private final String delayedRemovalClientId = "delayedRemovalClientId";

    @Before
    public void before() throws Throwable {
        super.before();
        grpcClientSettingsManager = spy(new GrpcClientSettingsManager(messagingProcessor));
    }

    @After
    public void cleanUpClientSettings() {
        grpcClientSettingsManager.removeAndGetRawClientSettings(CLIENT_ID);
        grpcClientSettingsManager.removeAndGetRawClientSettings(generationClientId);
        grpcClientSettingsManager.removeAndGetRawClientSettings(sameGenerationClientId);
        grpcClientSettingsManager.removeAndGetRawClientSettings(legacyClientId);
        grpcClientSettingsManager.removeAndGetRawClientSettings(invalidGenerationClientId);
        grpcClientSettingsManager.removeAndGetRawClientSettings(unconditionalRemovalClientId);
        grpcClientSettingsManager.removeAndGetRawClientSettings(cleanerProducerClientId);
        grpcClientSettingsManager.removeAndGetRawClientSettings(cleanerConsumerClientId);
        grpcClientSettingsManager.removeAndGetRawClientSettings(instanceIsolationClientId);
        grpcClientSettingsManager.removeAndGetRawClientSettings(delayedRemovalClientId);
    }

    @Test
    public void testGetProducerData() {
        ProxyContext context = ProxyContext.create().withVal(ContextVariable.CLIENT_ID, CLIENT_ID);

        this.grpcClientSettingsManager.updateClientSettings(context, CLIENT_ID, Settings.newBuilder()
            .setBackoffPolicy(RetryPolicy.getDefaultInstance())
            .setPublishing(Publishing.getDefaultInstance())
            .build());
        Settings settings = this.grpcClientSettingsManager.getClientSettings(context);
        assertNotEquals(settings.getBackoffPolicy(), settings.getBackoffPolicy().getDefaultInstanceForType());
        assertNotEquals(settings.getPublishing(), settings.getPublishing().getDefaultInstanceForType());
    }

    @Test
    public void testGetSubscriptionData() {
        ProxyContext context = ProxyContext.create().withVal(ContextVariable.CLIENT_ID, CLIENT_ID);

        SubscriptionGroupConfig subscriptionGroupConfig = new SubscriptionGroupConfig();
        when(this.messagingProcessor.getSubscriptionGroupConfig(any(), any()))
            .thenReturn(subscriptionGroupConfig);

        this.grpcClientSettingsManager.updateClientSettings(context, CLIENT_ID, Settings.newBuilder()
            .setSubscription(Subscription.newBuilder()
                .setGroup(Resource.newBuilder().setName("group").build())
                .build())
            .build());

        Settings settings = this.grpcClientSettingsManager.getClientSettings(context);
        assertEquals(settings.getBackoffPolicy(), this.grpcClientSettingsManager.createDefaultConsumerSettingsBuilder().build().getBackoffPolicy());

        subscriptionGroupConfig.setRetryMaxTimes(3);
        subscriptionGroupConfig.getGroupRetryPolicy().setType(GroupRetryPolicyType.CUSTOMIZED);
        subscriptionGroupConfig.getGroupRetryPolicy().setCustomizedRetryPolicy(new CustomizedRetryPolicy(new long[] {1000}));
        settings = this.grpcClientSettingsManager.getClientSettings(context);
        assertEquals(RetryPolicy.newBuilder()
            .setMaxAttempts(4)
            .setCustomizedBackoff(CustomizedBackoff.newBuilder()
                .addNext(Durations.fromSeconds(1))
                .build())
            .build(), settings.getBackoffPolicy());

        subscriptionGroupConfig.setRetryMaxTimes(10);
        subscriptionGroupConfig.getGroupRetryPolicy().setType(GroupRetryPolicyType.EXPONENTIAL);
        subscriptionGroupConfig.getGroupRetryPolicy().setExponentialRetryPolicy(new ExponentialRetryPolicy(1000, 2000, 3));
        settings = this.grpcClientSettingsManager.getClientSettings(context);
        assertEquals(RetryPolicy.newBuilder()
            .setMaxAttempts(11)
            .setExponentialBackoff(ExponentialBackoff.newBuilder()
                .setMax(Durations.fromSeconds(2))
                .setInitial(Durations.fromSeconds(1))
                .setMultiplier(3)
                .build())
            .build(), settings.getBackoffPolicy());

        Settings settings1 = this.grpcClientSettingsManager.removeAndGetClientSettings(context);
        assertEquals(settings, settings1);

        assertNull(this.grpcClientSettingsManager.getClientSettings(context));
        assertNull(this.grpcClientSettingsManager.removeAndGetClientSettings(context));
    }

    @Test
    public void testGenerationOwnershipPreventsStaleUpdateAndRemoval() {
        Settings oldSettings = Settings.newBuilder()
            .setClientType(ClientType.PRODUCER)
            .build();
        Settings newSettings = Settings.newBuilder()
            .setClientType(ClientType.SIMPLE_CONSUMER)
            .build();

        grpcClientSettingsManager.updateClientSettings(ctx, generationClientId, 1, oldSettings);
        grpcClientSettingsManager.updateClientSettings(ctx, generationClientId, 2, newSettings);

        assertNull(grpcClientSettingsManager.getRawClientSettings(generationClientId, 1));
        assertEquals(newSettings, grpcClientSettingsManager.getRawClientSettings(generationClientId, 2));
        assertNull(grpcClientSettingsManager.removeAndGetRawClientSettings(generationClientId, 1));
        assertEquals(newSettings, grpcClientSettingsManager.getRawClientSettings(generationClientId));

        grpcClientSettingsManager.updateClientSettings(ctx, generationClientId, 1, oldSettings);
        assertEquals(newSettings, grpcClientSettingsManager.getRawClientSettings(generationClientId));
        assertEquals(newSettings,
            grpcClientSettingsManager.removeAndGetRawClientSettings(generationClientId, 2));
        assertNull(grpcClientSettingsManager.getRawClientSettings(generationClientId));
    }

    @Test
    public void testGenerationOwnershipIsIsolatedBetweenManagerInstances() {
        GrpcClientSettingsManager managerA = new GrpcClientSettingsManager(messagingProcessor);
        GrpcClientSettingsManager managerB = new GrpcClientSettingsManager(messagingProcessor);
        Settings settingsA = Settings.newBuilder()
            .setClientType(ClientType.PRODUCER)
            .build();
        Settings settingsB = Settings.newBuilder()
            .setClientType(ClientType.SIMPLE_CONSUMER)
            .build();

        managerA.updateClientSettings(ctx, instanceIsolationClientId, 2, settingsA);
        managerB.updateClientSettings(ctx, instanceIsolationClientId, 1, settingsB);

        assertEquals(settingsA, managerA.getRawClientSettings(instanceIsolationClientId, 2));
        assertEquals(settingsB, managerB.getRawClientSettings(instanceIsolationClientId, 1));
    }

    @Test
    public void testDelayedConditionalRemovalIsIsolatedBetweenManagerInstances() {
        GrpcClientSettingsManager managerA = new GrpcClientSettingsManager(messagingProcessor);
        GrpcClientSettingsManager managerB = new GrpcClientSettingsManager(messagingProcessor);
        Settings settingsA = Settings.newBuilder()
            .setClientType(ClientType.PRODUCER)
            .build();
        Settings settingsB = Settings.newBuilder()
            .setClientType(ClientType.SIMPLE_CONSUMER)
            .build();

        managerA.updateClientSettings(ctx, delayedRemovalClientId, 1, settingsA);
        managerB.updateClientSettings(ctx, delayedRemovalClientId, 1, settingsB);

        assertEquals(settingsA, managerA.removeAndGetRawClientSettings(delayedRemovalClientId, 1));
        assertEquals(settingsB, managerB.getRawClientSettings(delayedRemovalClientId, 1));
    }

    @Test
    public void testSameGenerationMayUpdateSettings() {
        Settings oldSettings = Settings.newBuilder()
            .setClientType(ClientType.PRODUCER)
            .build();
        Settings newSettings = Settings.newBuilder()
            .setClientType(ClientType.SIMPLE_CONSUMER)
            .build();

        grpcClientSettingsManager.updateClientSettings(ctx, sameGenerationClientId, 1, oldSettings);
        grpcClientSettingsManager.updateClientSettings(ctx, sameGenerationClientId, 1, newSettings);

        assertEquals(newSettings,
            grpcClientSettingsManager.removeAndGetRawClientSettings(sameGenerationClientId, 1));
    }

    @Test
    public void testLegacyUpdateUnconditionallyReplacesOwnedSettings() {
        Settings ownedSettings = Settings.newBuilder()
            .setClientType(ClientType.PRODUCER)
            .build();
        Settings legacySettings = Settings.newBuilder()
            .setClientType(ClientType.SIMPLE_CONSUMER)
            .build();

        grpcClientSettingsManager.updateClientSettings(ctx, legacyClientId, 2, ownedSettings);
        grpcClientSettingsManager.updateClientSettings(ctx, legacyClientId, legacySettings);

        assertEquals(legacySettings, grpcClientSettingsManager.getRawClientSettings(legacyClientId, 2));
        assertEquals(legacySettings, grpcClientSettingsManager.getRawClientSettings(legacyClientId, 0));
        assertEquals(legacySettings, grpcClientSettingsManager.getRawClientSettings(legacyClientId, 3));
        assertEquals(legacySettings,
            grpcClientSettingsManager.removeAndGetRawClientSettings(legacyClientId, 0));
    }

    @Test
    public void testGenerationAwareUpdateRequiresPositiveGeneration() {
        Settings settings = Settings.newBuilder()
            .setClientType(ClientType.PRODUCER)
            .build();

        assertThrows(IllegalArgumentException.class,
            () -> grpcClientSettingsManager.updateClientSettings(ctx, invalidGenerationClientId, 0, settings));
        assertThrows(IllegalArgumentException.class,
            () -> grpcClientSettingsManager.updateClientSettings(ctx, invalidGenerationClientId, -1, settings));
        assertNull(grpcClientSettingsManager.getRawClientSettings(invalidGenerationClientId));
    }

    @Test
    public void testUnconditionalRemovalRetainsLegacyBehavior() {
        Settings settings = Settings.newBuilder()
            .setClientType(ClientType.PRODUCER)
            .build();

        grpcClientSettingsManager.updateClientSettings(ctx, unconditionalRemovalClientId, 2, settings);

        assertEquals(settings,
            grpcClientSettingsManager.removeAndGetRawClientSettings(unconditionalRemovalClientId));
        assertNull(grpcClientSettingsManager.getRawClientSettings(unconditionalRemovalClientId));
    }

    @Test
    public void testCleanerHandlesGenerationOwnedSettings() {
        Settings producerSettings = Settings.newBuilder()
            .setClientType(ClientType.PRODUCER)
            .build();
        Settings consumerSettings = Settings.newBuilder()
            .setClientType(ClientType.PUSH_CONSUMER)
            .setSubscription(Subscription.newBuilder()
                .setGroup(Resource.newBuilder().setName("group").build())
                .build())
            .build();

        grpcClientSettingsManager.updateClientSettings(ctx, cleanerProducerClientId, 1, producerSettings);
        grpcClientSettingsManager.updateClientSettings(ctx, cleanerConsumerClientId, 1, consumerSettings);

        grpcClientSettingsManager.onWaitEnd();

        assertEquals(producerSettings, grpcClientSettingsManager.getRawClientSettings(cleanerProducerClientId));
        assertNull(grpcClientSettingsManager.getRawClientSettings(cleanerConsumerClientId));
    }

    @Test
    public void testOfflineClientLiteSubscription_SettingsNullAndNoCachedSettings() {
        doReturn(null).when(grpcClientSettingsManager).getRawClientSettings(anyString());

        grpcClientSettingsManager.offlineClientLiteSubscription(ctx, clientId, null);

        verify(messagingProcessor, never()).syncLiteSubscription(any(), any(), anyLong());
    }

    @Test
    public void testOfflineClientLiteSubscription_SettingsNull_CachedSettingsNotLite() {
        Settings cachedSettings = Settings.newBuilder()
            .setClientType(ClientType.PRODUCER)
            .build();
        doReturn(cachedSettings).when(grpcClientSettingsManager).getRawClientSettings(anyString());

        grpcClientSettingsManager.offlineClientLiteSubscription(ctx, clientId, null);

        verify(messagingProcessor, never()).syncLiteSubscription(any(), any(), anyLong());
    }

    @Test
    public void testOfflineClientLiteSubscription_SettingsNotNull_NotLiteConsumer() {
        Settings settings = Settings.newBuilder()
            .setClientType(ClientType.PUSH_CONSUMER)
            .build();

        grpcClientSettingsManager.offlineClientLiteSubscription(ctx, clientId, settings);

        verify(messagingProcessor, never()).syncLiteSubscription(any(), any(), anyLong());
    }

    @Test
    public void testOfflineClientLiteSubscription_ValidLiteConsumer_Success() {
        Subscription subscription = Subscription.newBuilder()
            .setGroup(Resource.newBuilder().setName("testGroup").build())
            .addSubscriptions(SubscriptionEntry.newBuilder()
                .setTopic(Resource.newBuilder().setName("testTopic").build())
                .build())
            .build();

        Settings settings = Settings.newBuilder()
            .setClientType(ClientType.LITE_PUSH_CONSUMER)
            .setSubscription(subscription)
            .build();

        when(messagingProcessor.syncLiteSubscription(any(), any(LiteSubscriptionDTO.class), anyLong()))
            .thenReturn(CompletableFuture.completedFuture(null));

        grpcClientSettingsManager.offlineClientLiteSubscription(ctx, clientId, settings);

        verify(messagingProcessor, times(1)).syncLiteSubscription(any(), any(LiteSubscriptionDTO.class), anyLong());
    }

    @Test
    public void testOfflineClientLiteSubscription_ValidLiteConsumer_SyncThrowsException() {
        Subscription subscription = Subscription.newBuilder()
            .setGroup(Resource.newBuilder().setName("testGroup").build())
            .addSubscriptions(SubscriptionEntry.newBuilder()
                .setTopic(Resource.newBuilder().setName("testTopic").build())
                .build())
            .build();

        Settings settings = Settings.newBuilder()
            .setClientType(ClientType.LITE_PUSH_CONSUMER)
            .setSubscription(subscription)
            .build();

        CompletableFuture<Void> future = new CompletableFuture<>();
        future.completeExceptionally(new RuntimeException("Simulated error"));
        when(messagingProcessor.syncLiteSubscription(any(), any(LiteSubscriptionDTO.class), anyLong()))
            .thenReturn(future);

        grpcClientSettingsManager.offlineClientLiteSubscription(ctx, clientId, settings);

        verify(messagingProcessor, times(1)).syncLiteSubscription(any(), any(LiteSubscriptionDTO.class), anyLong());
    }
}
