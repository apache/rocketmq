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

package org.apache.rocketmq.broker.filtersrv;

import io.netty.channel.Channel;
import java.util.List;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.BrokerConfig;
import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class FilterServerManagerTest {

    @Mock
    private BrokerController brokerController;

    private FilterServerManager filterServerManager;

    private BrokerConfig brokerConfig = new BrokerConfig();

    @Mock
    private Channel channel;

    private static final String FILTER_SERVER_ADDR = "192.168.1.1";

    @Before
    public void before() throws InterruptedException {
        when(brokerController.getBrokerConfig()).thenReturn(brokerConfig);
        filterServerManager = new FilterServerManager(brokerController);
        filterServerManager.start();
        filterServerManager.registerFilterServer(channel, FILTER_SERVER_ADDR);
    }

    @After
    public void after() {
        filterServerManager.shutdown();
        brokerController.shutdown();
    }

    @Test
    public void createFilterServerTest() {
        Assertions.assertThatCode(() ->  filterServerManager.createFilterServer()).doesNotThrowAnyException();
    }

    @Test
    public void registerFilterServerTest() {
        Assertions.assertThatCode(() ->  filterServerManager.registerFilterServer(channel, FILTER_SERVER_ADDR)).doesNotThrowAnyException();
    }

    @Test
    public void scanNotActiveChannelTest() {
        Assertions.assertThatCode(() ->  filterServerManager.scanNotActiveChannel()).doesNotThrowAnyException();
    }

    @Test
    public void doChannelCloseEventTest() {
        Assertions.assertThatCode(() -> filterServerManager.doChannelCloseEvent(FILTER_SERVER_ADDR, channel)).doesNotThrowAnyException();
    }

    @Test
    public void buildNewFilterServerListTest() {
        final List<String> filterServerList = filterServerManager.buildNewFilterServerList();
        assert !filterServerList.isEmpty();
    }
}
