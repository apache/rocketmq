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
package org.apache.rocketmq.namesrv.routeinfo;

import com.alibaba.fastjson2.JSONWriter;
import io.netty.channel.Channel;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.common.namesrv.NamesrvConfig;
import org.apache.rocketmq.namesrv.route.ZoneRouteRPCHook;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.body.TopicConfigAndMappingSerializeWrapper;
import org.apache.rocketmq.remoting.protocol.body.TopicConfigSerializeWrapper;
import org.apache.rocketmq.remoting.protocol.route.QueueData;
import org.apache.rocketmq.remoting.protocol.route.TopicRouteData;
import org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingInfo;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class RouteInfoManagerCacheTest {
    private NamesrvConfig config;
    private RouteInfoManager manager;
    private Channel channel;

    @Before
    public void setup() {
        config = new NamesrvConfig();
        config.setRouteInfoCacheMaxBytes(1024 * 1024);
        manager = new RouteInfoManager(config, null);
        channel = mock(Channel.class);
        register(0, "address0", wrapper(4), "zone", Collections.emptyList());
    }

    private TopicConfigSerializeWrapper wrapper(int queues) {
        TopicConfigSerializeWrapper wrapper = new TopicConfigSerializeWrapper();
        wrapper.getTopicConfigTable().put("topic", new TopicConfig("topic", queues, queues));
        wrapper.getTopicConfigTable().put("other", new TopicConfig("other", queues, queues));
        return wrapper;
    }

    private void register(long id, String address, TopicConfigSerializeWrapper wrapper, String zone, java.util.List<String> filters) {
        manager.registerBroker("cluster", address, "broker", id, address, zone, null, true, wrapper, filters, channel);
    }

    private byte[] body() {
        return manager.pickupTopicRouteDataEncoded("topic", null, true);
    }

    private TopicRouteData decode(byte[] body) {
        return TopicRouteData.decode(body, TopicRouteData.class);
    }

    @Test
    public void testReuseAcrossUnchangedRegistrationsAndHeartbeats() {
        byte[] first = body();
        assertSame(first, body());
        register(0, "address0", wrapper(4), "zone", Collections.emptyList());
        manager.updateBrokerInfoUpdateTimestamp("cluster", "address0");
        assertSame(first, body());
        assertArrayEquals(manager.pickupTopicRouteData("topic").encode(),
            manager.pickupTopicRouteDataEncoded("topic", null, false));
    }

    @Test
    public void testRegistrationUpdatesQueueAddressZoneAndFilters() {
        byte[] previous = body();
        TopicConfigSerializeWrapper changed = wrapper(8);
        changed.getDataVersion().nextVersion();
        register(0, "newAddress", changed, "newZone", Arrays.asList("filter"));
        byte[] next = body();
        assertNotSame(previous, next);
        TopicRouteData route = decode(next);
        assertEquals(8, route.getQueueDatas().get(0).getReadQueueNums());
        assertEquals("newAddress", route.getBrokerDatas().get(0).getBrokerAddrs().get(0L));
        assertEquals("newZone", route.getBrokerDatas().get(0).getZoneName());
        assertEquals(Arrays.asList("filter"), route.getFilterServerTable().get("newAddress"));
    }

    @Test
    public void testMetadataChangesInvalidateWithoutQueueChanges() {
        byte[] previous = body();
        register(0, "address0", wrapper(4), "newZone", Collections.emptyList());
        assertNotSame(previous, body());
        assertEquals("newZone", decode(body()).getBrokerDatas().get(0).getZoneName());
        previous = body();
        register(0, "address0", wrapper(4), "newZone", Arrays.asList("filter"));
        assertNotSame(previous, body());
        assertEquals(Arrays.asList("filter"), decode(body()).getFilterServerTable().get("address0"));
        previous = body();
        register(0, "address0", wrapper(4), "newZone", Collections.emptyList());
        assertNotSame(previous, body());
        assertTrue(decode(body()).getFilterServerTable().isEmpty());
    }

    @Test
    public void testQueueChangeAndUnregisterLastBrokerInvalidate() {
        byte[] previous = body();
        TopicConfigSerializeWrapper changed = wrapper(8);
        changed.getDataVersion().nextVersion();
        register(0, "address0", changed, "zone", Collections.emptyList());
        assertNotSame(previous, body());
        assertEquals(8, decode(body()).getQueueDatas().get(0).getReadQueueNums());
        manager.unregisterBroker("cluster", "address0", "broker", 0);
        assertNull(body());
    }

    @Test
    public void testZoneFilteringDoesNotMutateSharedBody() {
        manager.registerBroker("cluster", "address2", "broker2", 0, "ha", "otherZone", null,
            true, wrapper(4), Collections.emptyList(), channel);
        byte[] cached = body();
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_ROUTEINFO_BY_TOPIC, null);
        request.addExtField(MixAll.ZONE_MODE, "true");
        request.addExtField(MixAll.ZONE_NAME, "zone");
        RemotingCommand response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);
        response.setBody(cached);
        new ZoneRouteRPCHook().doAfterResponse("client", request, response);
        assertEquals(1, decode(response.getBody()).getBrokerDatas().size());
        assertSame(cached, body());
        assertEquals(2, decode(body()).getBrokerDatas().size());
    }

    @Test
    public void testOrderConfigAndActingMasterSwitch() {
        byte[] original = body();
        byte[] ordered = manager.pickupTopicRouteDataEncoded("topic", "broker:4", true);
        assertEquals("broker:4", decode(ordered).getOrderTopicConf());
        assertNull(decode(body()).getOrderTopicConf());
        config.setSupportActingMaster(true);
        assertNotSame(original, body());
        register(1, "address1", wrapper(4), "zone", Collections.emptyList());
        manager.unregisterBroker("cluster", "address0", "broker", 0);
        TopicRouteData route = decode(body());
        assertEquals("address1", route.getBrokerDatas().get(0).getBrokerAddrs().get(0L));
        assertFalse(PermName.isWriteable(route.getQueueDatas().get(0).getPerm()));
        config.setSupportActingMaster(false);
        assertEquals("address1", decode(body()).getBrokerDatas().get(0).getBrokerAddrs().get(1L));
    }

    @Test
    public void testTopicDeletionRegistrationAndPermissions() {
        byte[] initial = body();
        manager.wipeWritePermOfBrokerByLock("broker");
        assertNotSame(initial, body());
        assertFalse(PermName.isWriteable(decode(body()).getQueueDatas().get(0).getPerm()));
        manager.addWritePermOfBrokerByLock("broker");
        assertTrue(PermName.isWriteable(decode(body()).getQueueDatas().get(0).getPerm()));
        manager.deleteTopic("topic", "cluster");
        assertNull(body());
        QueueData queue = new QueueData();
        queue.setBrokerName("broker");
        queue.setReadQueueNums(3);
        manager.registerTopic("topic", Collections.singletonList(queue));
        assertEquals(3, decode(body()).getQueueDatas().get(0).getReadQueueNums());
        manager.deleteTopic("topic");
        assertNull(body());
    }

    @Test
    public void testRegistrationDeletesTopic() {
        body();
        config.setDeleteTopicWithBrokerRegistration(true);
        TopicConfigSerializeWrapper wrapper = wrapper(4);
        wrapper.getTopicConfigTable().remove("topic");
        wrapper.getDataVersion().nextVersion();
        register(0, "address0", wrapper, "zone", Collections.emptyList());
        assertNull(body());
    }

    @Test
    public void testStaticMappingAndFilterRemoval() {
        TopicConfigAndMappingSerializeWrapper wrapper = new TopicConfigAndMappingSerializeWrapper();
        wrapper.setTopicConfigTable(wrapper(4).getTopicConfigTable());
        TopicQueueMappingInfo mapping = new TopicQueueMappingInfo("topic", 4, "broker", 1);
        mapping.getCurrIdMap().put(0, 1);
        wrapper.getTopicQueueMappingInfoMap().put("topic", mapping);
        wrapper.getDataVersion().nextVersion();
        register(0, "address0", wrapper, "zone", Arrays.asList("filter"));
        byte[] first = body();
        TopicQueueMappingInfo changed = new TopicQueueMappingInfo("topic", 8, "broker", 2);
        // A new registration has a separately decoded DataVersion, not the live table's object.
        wrapper = new TopicConfigAndMappingSerializeWrapper();
        wrapper.setTopicConfigTable(wrapper(4).getTopicConfigTable());
        wrapper.getTopicQueueMappingInfoMap().put("topic", changed);
        wrapper.getDataVersion().nextVersion();
        wrapper.getDataVersion().nextVersion();
        register(0, "address0", wrapper, "zone", Collections.emptyList());
        assertNotSame(first, body());
        TopicRouteData route = decode(body());
        assertEquals(2, route.getTopicQueueMappingByBroker().get("broker").getEpoch());
        assertTrue(route.getFilterServerTable().isEmpty());
    }

    @Test
    public void testDisabledAndTooSmallCache() {
        for (int limit : new int[] {0, 1}) {
            config.setRouteInfoCacheMaxBytes(limit);
            manager = new RouteInfoManager(config, null);
            register(0, "address0", wrapper(4), "zone", Collections.emptyList());
            assertNotSame(body(), body());
            assertArrayEquals(body(), body());
        }
    }

    @Test
    public void testInterruptedMissReleasesOnlyAcquiredLocks() {
        Thread.currentThread().interrupt();
        try {
            assertNull(body());
            assertTrue(Thread.currentThread().isInterrupted());
        } finally {
            Thread.interrupted();
        }
        assertEquals(4, decode(body()).getQueueDatas().get(0).getReadQueueNums());
    }

    @Test
    public void testFillRacingCompletedMutationIsNotReused() throws Exception {
        CountDownLatch captured = new CountDownLatch(1);
        CountDownLatch resume = new CountDownLatch(1);
        manager = new RouteInfoManager(config, null) {
            @Override
            public TopicRouteData pickupTopicRouteData(String topic) {
                TopicRouteData route = super.pickupTopicRouteData(topic);
                if (route == null) {
                    return null;
                }
                return new TopicRouteData(route) {
                    @Override
                    public byte[] encode(JSONWriter.Feature... features) {
                        captured.countDown();
                        try {
                            assertTrue(resume.await(10, TimeUnit.SECONDS));
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new IllegalStateException(e);
                        }
                        return super.encode(features);
                    }
                };
            }
        };
        register(0, "address0", wrapper(4), "zone", Collections.emptyList());
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<byte[]> old = executor.submit(this::body);
            assertTrue(captured.await(10, TimeUnit.SECONDS));
            // This must finish while JSON encoding is paused: no route read lock is held.
            manager.deleteTopic("topic");
            resume.countDown();
            old.get(10, TimeUnit.SECONDS);
            assertNull(body());
        } finally {
            resume.countDown();
            executor.shutdownNow();
        }
    }
}
