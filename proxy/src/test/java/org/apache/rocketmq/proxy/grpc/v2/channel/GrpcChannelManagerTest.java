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

package org.apache.rocketmq.proxy.grpc.v2.channel;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.config.InitConfigTest;
import org.apache.rocketmq.proxy.grpc.v2.common.GrpcClientSettingsManager;
import org.apache.rocketmq.proxy.service.relay.ProxyRelayResult;
import org.apache.rocketmq.proxy.service.relay.ProxyRelayService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

@RunWith(MockitoJUnitRunner.class)
public class GrpcChannelManagerTest extends InitConfigTest {

    @Mock
    private ProxyRelayService proxyRelayService;
    @Mock
    private GrpcClientSettingsManager grpcClientSettingsManager;

    private GrpcChannelManager grpcChannelManager;

    @Before
    public void before() throws Throwable {
        super.before();
        this.grpcChannelManager = new GrpcChannelManager(proxyRelayService, grpcClientSettingsManager);
    }

    @After
    public void after() {
        if (this.grpcChannelManager != null) {
            try {
                this.grpcChannelManager.shutdown();
            } catch (Exception ignore) {
            }
        }
        super.after();
    }

    private ProxyContext createContext() {
        return ProxyContext.create()
            .setRemoteAddress("10.152.39.53:9768")
            .setLocalAddress("11.193.0.1:1210");
    }

    @Test
    public void testCreateAndGetChannel() {
        String clientIdA = RandomStringUtils.randomAlphabetic(10);
        String clientIdB = RandomStringUtils.randomAlphabetic(10);

        assertEquals(0, this.grpcChannelManager.getChannelCount());

        this.grpcChannelManager.createChannel(createContext(), clientIdA);
        this.grpcChannelManager.createChannel(createContext(), clientIdB);

        assertEquals(2, this.grpcChannelManager.getChannelCount());
        assertNotNull(this.grpcChannelManager.getChannel(clientIdA));
        assertNotNull(this.grpcChannelManager.getChannel(clientIdB));
        assertNull(this.grpcChannelManager.getChannel("notExistClientId"));

        Set<String> activeClientIdSet = this.grpcChannelManager.getActiveClientIdSet();
        assertEquals(2, activeClientIdSet.size());
        assertTrue(activeClientIdSet.contains(clientIdA));
        assertTrue(activeClientIdSet.contains(clientIdB));
    }

    @Test
    public void testCreateChannelIdempotent() {
        String clientId = RandomStringUtils.randomAlphabetic(10);

        this.grpcChannelManager.createChannel(createContext(), clientId);
        this.grpcChannelManager.createChannel(createContext(), clientId);

        assertEquals(1, this.grpcChannelManager.getChannelCount());
    }

    @Test
    public void testRemoveChannel() {
        String clientId = RandomStringUtils.randomAlphabetic(10);

        this.grpcChannelManager.createChannel(createContext(), clientId);
        assertEquals(1, this.grpcChannelManager.getChannelCount());

        assertNotNull(this.grpcChannelManager.removeChannel(clientId));
        assertEquals(0, this.grpcChannelManager.getChannelCount());
        assertNull(this.grpcChannelManager.getChannel(clientId));
        assertFalse(this.grpcChannelManager.getActiveClientIdSet().contains(clientId));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testActiveClientIdSetUnmodifiable() {
        String clientId = RandomStringUtils.randomAlphabetic(10);
        this.grpcChannelManager.createChannel(createContext(), clientId);

        this.grpcChannelManager.getActiveClientIdSet().add("anotherClientId");
    }

    @Test
    public void testActiveClientIdSetSnapshot() {
        String clientId = RandomStringUtils.randomAlphabetic(10);
        this.grpcChannelManager.createChannel(createContext(), clientId);

        Set<String> snapshot = this.grpcChannelManager.getActiveClientIdSet();
        assertEquals(1, snapshot.size());

        this.grpcChannelManager.removeChannel(clientId);
        assertEquals(0, this.grpcChannelManager.getChannelCount());

        assertTrue(snapshot.contains(clientId));
    }

    @Test
    public void testPendingResponseFutureCount() {
        assertEquals(0, this.grpcChannelManager.getPendingResponseFutureCount());

        String nonce = this.grpcChannelManager.addResponseFuture(new CompletableFuture<ProxyRelayResult<Void>>());
        assertEquals(1, this.grpcChannelManager.getPendingResponseFutureCount());

        assertNotNull(this.grpcChannelManager.getAndRemoveResponseFuture(nonce));
        assertEquals(0, this.grpcChannelManager.getPendingResponseFutureCount());
        assertNull(this.grpcChannelManager.getAndRemoveResponseFuture(nonce));
    }

    @Test
    public void testGetAndRemoveResponseFuture() {
        CompletableFuture<ProxyRelayResult<Void>> future = new CompletableFuture<>();
        String nonce = this.grpcChannelManager.addResponseFuture(future);
        assertEquals(1, this.grpcChannelManager.getPendingResponseFutureCount());

        CompletableFuture<ProxyRelayResult<Void>> removed = this.grpcChannelManager.getAndRemoveResponseFuture(nonce);
        assertSame(future, removed);
        assertEquals(0, this.grpcChannelManager.getPendingResponseFutureCount());
        assertNull(this.grpcChannelManager.getAndRemoveResponseFuture(nonce));
    }
}
