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

import apache.rocketmq.v2.CustomizedBackoff;
import apache.rocketmq.v2.ExponentialBackoff;
import apache.rocketmq.v2.Publishing;
import apache.rocketmq.v2.Resource;
import apache.rocketmq.v2.RetryPolicy;
import apache.rocketmq.v2.Settings;
import apache.rocketmq.v2.Subscription;
import com.google.protobuf.util.Durations;
import org.apache.rocketmq.proxy.common.ContextVariable;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.grpc.v2.BaseActivityTest;
import org.apache.rocketmq.remoting.protocol.subscription.CustomizedRetryPolicy;
import org.apache.rocketmq.remoting.protocol.subscription.ExponentialRetryPolicy;
import org.apache.rocketmq.remoting.protocol.subscription.GroupRetryPolicyType;
import org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

public class GrpcClientSettingsManagerTest extends BaseActivityTest {
    private GrpcClientSettingsManager grpcClientSettingsManager;

    @Before
    public void before() throws Throwable {
        super.before();
        this.grpcClientSettingsManager = new GrpcClientSettingsManager(this.messagingProcessor);
    }

    @Test
    public void testGetProducerData() {
        ProxyContext context = ProxyContext.create().withVal(ContextVariable.CLIENT_ID, CLIENT_ID);

        this.grpcClientSettingsManager.updateClientSettings(CLIENT_ID, Settings.newBuilder()
            .setBackoffPolicy(RetryPolicy.getDefaultInstance())
            .setPublishing(Publishing.getDefaultInstance())
            .build());
        Settings settings = this.grpcClientSettingsManager.getClientSettings(context);
        assertNotEquals(settings.getBackoffPolicy(), settings.getBackoffPolicy().getDefaultInstanceForType());
        assertNotEquals(settings.getPublishing(), settings.getPublishing().getDefaultInstanceForType());
    }

    @Test
    public void testGetSubscriptionData() {
        SubscriptionGroupConfig subscriptionGroupConfig = new SubscriptionGroupConfig();
        when(this.messagingProcessor.getSubscriptionGroupConfig(any(), any()))
            .thenReturn(subscriptionGroupConfig);

        this.grpcClientSettingsManager.updateClientSettings(CLIENT_ID, Settings.newBuilder()
            .setSubscription(Subscription.newBuilder()
                .setGroup(Resource.newBuilder().setName("group").build())
                .build())
            .build());

        ProxyContext context = ProxyContext.create().withVal(ContextVariable.CLIENT_ID, CLIENT_ID);

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
}
