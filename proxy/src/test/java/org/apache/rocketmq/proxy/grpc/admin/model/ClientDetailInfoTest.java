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

package org.apache.rocketmq.proxy.grpc.admin.model;

import java.util.Arrays;
import java.util.Collections;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class ClientDetailInfoTest {

    @Test
    public void testDefaultConstructor() {
        ClientDetailInfo detail = new ClientDetailInfo();
        assertNull(detail.getClientInstance());
        assertNull(detail.getSettings());
        assertNull(detail.getHeartbeatHistory());
        assertNull(detail.getAuthStatus());
        assertNull(detail.getConsumeProgress());
        assertNull(detail.getNetworkInfo());
    }

    @Test
    public void testSettersAndGetters() {
        ClientDetailInfo detail = new ClientDetailInfo();

        ClientInstanceInfo instance = new ClientInstanceInfo();
        instance.setClientId("client-1");
        detail.setClientInstance(instance);
        assertSame(instance, detail.getClientInstance());

        ClientDetailInfo.ClientSettingsInfo settings = new ClientDetailInfo.ClientSettingsInfo();
        detail.setSettings(settings);
        assertSame(settings, detail.getSettings());

        ClientDetailInfo.HeartbeatRecordInfo record = new ClientDetailInfo.HeartbeatRecordInfo();
        detail.setHeartbeatHistory(Collections.singletonList(record));
        assertEquals(1, detail.getHeartbeatHistory().size());
        assertSame(record, detail.getHeartbeatHistory().get(0));

        ClientDetailInfo.AuthStatusInfo authStatus = new ClientDetailInfo.AuthStatusInfo();
        detail.setAuthStatus(authStatus);
        assertSame(authStatus, detail.getAuthStatus());

        ClientDetailInfo.ConsumeProgressInfo progress = new ClientDetailInfo.ConsumeProgressInfo();
        detail.setConsumeProgress(progress);
        assertSame(progress, detail.getConsumeProgress());

        ClientDetailInfo.NetworkInfoInfo networkInfo = new ClientDetailInfo.NetworkInfoInfo();
        detail.setNetworkInfo(networkInfo);
        assertSame(networkInfo, detail.getNetworkInfo());
    }

    @Test
    public void testClientSettingsInfo() {
        ClientDetailInfo.ClientSettingsInfo settings = new ClientDetailInfo.ClientSettingsInfo();
        settings.setSubscriptionMode("PUSH");
        settings.setReceiveBatchSize(32);
        settings.setLongPollingTimeoutMs(30000L);
        settings.setFifo(true);
        settings.setSubscriptionTopics(Arrays.asList("topicA", "topicB"));
        settings.setPublishingTopics(Collections.singletonList("topicC"));

        assertEquals("PUSH", settings.getSubscriptionMode());
        assertEquals(32, settings.getReceiveBatchSize());
        assertEquals(30000L, settings.getLongPollingTimeoutMs());
        assertTrue(settings.isFifo());
        assertEquals(Arrays.asList("topicA", "topicB"), settings.getSubscriptionTopics());
        assertEquals(Collections.singletonList("topicC"), settings.getPublishingTopics());
    }

    @Test
    public void testHeartbeatRecordInfo() {
        ClientDetailInfo.HeartbeatRecordInfo record = new ClientDetailInfo.HeartbeatRecordInfo();
        record.setTimestamp(1234567890L);
        record.setSuccess(true);
        record.setRemark("ok");

        assertEquals(1234567890L, record.getTimestamp());
        assertTrue(record.isSuccess());
        assertEquals("ok", record.getRemark());
    }

    @Test
    public void testAuthStatusInfo() {
        ClientDetailInfo.AuthStatusInfo authStatus = new ClientDetailInfo.AuthStatusInfo();
        authStatus.setAuthenticated(true);
        authStatus.setUsername("rocketmq");
        authStatus.setLastAuthTime(1234567890L);
        authStatus.setFailureReason("none");

        assertTrue(authStatus.isAuthenticated());
        assertEquals("rocketmq", authStatus.getUsername());
        assertEquals(1234567890L, authStatus.getLastAuthTime());
        assertEquals("none", authStatus.getFailureReason());
    }

    @Test
    public void testConsumeProgressInfo() {
        ClientDetailInfo.TopicConsumeProgressInfo topicProgress = new ClientDetailInfo.TopicConsumeProgressInfo();
        topicProgress.setTopic("topicA");
        topicProgress.setLag(100L);
        topicProgress.setLatencyMs(50L);

        assertEquals("topicA", topicProgress.getTopic());
        assertEquals(100L, topicProgress.getLag());
        assertEquals(50L, topicProgress.getLatencyMs());

        ClientDetailInfo.ConsumeProgressInfo progress = new ClientDetailInfo.ConsumeProgressInfo();
        progress.setLag(200L);
        progress.setLatencyMs(80L);
        progress.setTopicProgress(Collections.singletonList(topicProgress));

        assertEquals(200L, progress.getLag());
        assertEquals(80L, progress.getLatencyMs());
        assertEquals(1, progress.getTopicProgress().size());
        assertSame(topicProgress, progress.getTopicProgress().get(0));
    }

    @Test
    public void testNetworkInfoInfo() {
        ClientDetailInfo.NetworkInfoInfo networkInfo = new ClientDetailInfo.NetworkInfoInfo();
        networkInfo.setLocalAddress("127.0.0.1:8081");
        networkInfo.setRemoteAddress("127.0.0.1:12345");
        networkInfo.setRttMs(5L);
        networkInfo.setSslEnabled(false);

        assertEquals("127.0.0.1:8081", networkInfo.getLocalAddress());
        assertEquals("127.0.0.1:12345", networkInfo.getRemoteAddress());
        assertEquals(5L, networkInfo.getRttMs());
        assertFalse(networkInfo.isSslEnabled());
    }
}
