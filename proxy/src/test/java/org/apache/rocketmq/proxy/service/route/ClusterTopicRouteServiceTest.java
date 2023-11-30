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

package org.apache.rocketmq.proxy.service.route;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.net.HostAndPort;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.thread.ThreadPoolMonitor;
import org.apache.rocketmq.proxy.common.Address;
import org.apache.rocketmq.proxy.common.ProxyContext;
import org.apache.rocketmq.proxy.service.BaseServiceTest;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.route.BrokerData;
import org.apache.rocketmq.remoting.protocol.route.QueueData;
import org.apache.rocketmq.remoting.protocol.route.TopicRouteData;
import org.assertj.core.util.Lists;
import org.awaitility.Awaitility;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

public class ClusterTopicRouteServiceTest extends BaseServiceTest {

    private ClusterTopicRouteService topicRouteService;

    protected static final String BROKER2_NAME = "broker2";
    protected static final String BROKER2_ADDR = "127.0.0.2:10911";

    @Before
    public void before() throws Throwable {
        super.before();
        this.topicRouteService = new ClusterTopicRouteService(this.mqClientAPIFactory);

        when(this.mqClientAPIExt.getTopicRouteInfoFromNameServer(eq(TOPIC), anyLong())).thenReturn(topicRouteData);
        when(this.mqClientAPIExt.getTopicRouteInfoFromNameServer(eq(ERR_TOPIC), anyLong())).thenThrow(new MQClientException(ResponseCode.TOPIC_NOT_EXIST, ""));

        // build broker
        BrokerData brokerData = new BrokerData();
        brokerData.setCluster(CLUSTER_NAME);
        brokerData.setBrokerName(BROKER_NAME);
        HashMap<Long, String> brokerAddrs = new HashMap<>();
        brokerAddrs.put(MixAll.MASTER_ID, BROKER_ADDR);
        brokerData.setBrokerAddrs(brokerAddrs);

        // build broker2
        BrokerData broke2Data = new BrokerData();
        broke2Data.setCluster(CLUSTER_NAME);
        broke2Data.setBrokerName(BROKER2_NAME);
        HashMap<Long, String> broker2Addrs = new HashMap<>();
        broker2Addrs.put(MixAll.MASTER_ID, BROKER2_ADDR);
        broke2Data.setBrokerAddrs(broker2Addrs);

        // add brokers
        TopicRouteData brokerTopicRouteData = new TopicRouteData();
        brokerTopicRouteData.setBrokerDatas(Lists.newArrayList(brokerData, broke2Data));

        // add queue data
        QueueData queueData = new QueueData();
        queueData.setBrokerName(BROKER_NAME);

        QueueData queue2Data = new QueueData();
        queue2Data.setBrokerName(BROKER2_NAME);
        brokerTopicRouteData.setQueueDatas(Lists.newArrayList(queueData, queue2Data));
        when(this.mqClientAPIExt.getTopicRouteInfoFromNameServer(eq(BROKER_NAME), anyLong())).thenReturn(brokerTopicRouteData);
        when(this.mqClientAPIExt.getTopicRouteInfoFromNameServer(eq(BROKER2_NAME), anyLong())).thenReturn(brokerTopicRouteData);
    }

    @Test
    public void testGetCurrentMessageQueueView() throws Throwable {
        ProxyContext ctx = ProxyContext.create();
        MQClientException exception = catchThrowableOfType(() -> this.topicRouteService.getCurrentMessageQueueView(ctx, ERR_TOPIC), MQClientException.class);
        assertTrue(TopicRouteHelper.isTopicNotExistError(exception));
        assertEquals(1, this.topicRouteService.topicCache.asMap().size());

        assertNotNull(this.topicRouteService.getCurrentMessageQueueView(ctx, TOPIC));
        assertEquals(2, this.topicRouteService.topicCache.asMap().size());
    }

    @Test
    public void testGetBrokerAddr() throws Throwable {
        ProxyContext ctx = ProxyContext.create();
        assertEquals(BROKER_ADDR, topicRouteService.getBrokerAddr(ctx, BROKER_NAME));
        assertEquals(BROKER2_ADDR, topicRouteService.getBrokerAddr(ctx, BROKER2_NAME));
    }

    @Test
    public void testGetTopicRouteForProxy() throws Throwable {
        ProxyContext ctx = ProxyContext.create();
        List<Address> addressList = Lists.newArrayList(new Address(Address.AddressScheme.IPv4, HostAndPort.fromParts("127.0.0.1", 8888)));
        ProxyTopicRouteData proxyTopicRouteData = this.topicRouteService.getTopicRouteForProxy(ctx, addressList, TOPIC);

        assertEquals(1, proxyTopicRouteData.getBrokerDatas().size());
        assertEquals(addressList, proxyTopicRouteData.getBrokerDatas().get(0).getBrokerAddrs().get(MixAll.MASTER_ID));
    }

    @Test
    public void testTopicRouteCaffeineCache() throws InterruptedException {
        String key = "abc";
        String value = key;
        final AtomicBoolean throwException = new AtomicBoolean();
        ThreadPoolExecutor cacheRefreshExecutor = ThreadPoolMonitor.createAndMonitor(
            10, 10, 30L, TimeUnit.SECONDS, "test", 10);
        LoadingCache<String /* topicName */, String> topicCache = Caffeine.newBuilder().maximumSize(30).
            refreshAfterWrite(2, TimeUnit.SECONDS).executor(cacheRefreshExecutor).build(new CacheLoader<String, String>() {
                @Override public @Nullable String load(@NonNull String key) throws Exception {
                    try {
                        if (throwException.get()) {
                            throw new RuntimeException();
                        } else {
                            throwException.set(true);
                            return value;
                        }
                    } catch (Exception e) {
                        if (TopicRouteHelper.isTopicNotExistError(e)) {
                            return "";
                        }
                        throw e;
                    }
                }

                @Override
                public @Nullable String reload(@NonNull String key, @NonNull String oldValue) throws Exception {
                    try {
                        return load(key);
                    } catch (Exception e) {
                        return oldValue;
                    }
                }
            });
        assertThat(value).isEqualTo(topicCache.get(key));
        Awaitility.await().pollDelay(Duration.ofSeconds(5)).until(() -> true);
        assertThat(value).isEqualTo(topicCache.get(key));
    }
}
