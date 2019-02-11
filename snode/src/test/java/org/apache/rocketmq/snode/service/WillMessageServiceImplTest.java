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
import org.apache.rocketmq.common.message.mqtt.WillMessage;
import org.apache.rocketmq.remoting.ClientConfig;
import org.apache.rocketmq.remoting.ServerConfig;
import org.apache.rocketmq.snode.SnodeController;
import org.apache.rocketmq.snode.SnodeTestBase;
import org.apache.rocketmq.snode.service.impl.WillMessageServiceImpl;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class WillMessageServiceImplTest extends SnodeTestBase {

    @Spy
    private SnodeController snodeController = new SnodeController(new ServerConfig(),
            new ClientConfig(), new SnodeConfig());

    private WillMessageService willMessageService;

    @Before
    public void init() {
        willMessageService = new WillMessageServiceImpl(snodeController);
    }

    @Test
    public void saveWillMessageTest() {
        willMessageService.saveWillMessage("testClientId", new WillMessage());
    }

    @Test
    public void deleteWillMessageTest() {
        willMessageService.saveWillMessage("testClientId", new WillMessage());
        willMessageService.deleteWillMessage("testClientId");
    }

}
