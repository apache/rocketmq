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
package org.apache.rocketmq.snode.service;

import org.apache.rocketmq.common.SnodeConfig;
import org.apache.rocketmq.remoting.ClientConfig;
import org.apache.rocketmq.remoting.ServerConfig;
import org.apache.rocketmq.snode.SnodeController;
import org.apache.rocketmq.snode.client.SlowConsumerService;
import org.apache.rocketmq.snode.client.impl.SlowConsumerServiceImpl;
import org.apache.rocketmq.snode.offset.ConsumerOffsetManager;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class SlowConsumerServiceImplTest {
    @Spy
    private SnodeController snodeController = new SnodeController(new ServerConfig(), new ClientConfig(), new SnodeConfig());

    private final String enodeName = "testEndoe";

    private final String topic = "SnodeTopic";

    private final String group = "SnodeGroup";

    private final Integer queue = 1;

    private SlowConsumerService slowConsumerService;

    @Mock
    private ConsumerOffsetManager consumerOffsetManager;

    @Before
    public void init() {
        slowConsumerService = new SlowConsumerServiceImpl(snodeController);
    }

    @Test
    public void isSlowConsumerTest() {
        snodeController.setConsumerOffsetManager(consumerOffsetManager);
        when(snodeController.getConsumerOffsetManager().queryCacheOffset(anyString(), anyString(), anyString(), anyInt())).thenReturn(1024L);
        this.snodeController.getSnodeConfig().setSlowConsumerThreshold(100);
        boolean slowConsumer = slowConsumerService.isSlowConsumer(2000, topic, queue, group, enodeName);
        assertThat(slowConsumer).isTrue();
    }

    @Test
    public void isSlowConsumerTestFalse() {
        snodeController.setConsumerOffsetManager(consumerOffsetManager);
        when(snodeController.getConsumerOffsetManager().queryCacheOffset(anyString(), anyString(), anyString(), anyInt())).thenReturn(1024L);
        this.snodeController.getSnodeConfig().setSlowConsumerThreshold(100);
        boolean slowConsumer = slowConsumerService.isSlowConsumer(1025, topic, queue, group, enodeName);
        assertThat(slowConsumer).isFalse();
    }
}
