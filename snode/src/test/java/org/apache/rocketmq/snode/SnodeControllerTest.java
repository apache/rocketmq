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
package org.apache.rocketmq.snode;

import org.apache.rocketmq.common.MqttConfig;
import org.apache.rocketmq.common.SnodeConfig;
import org.apache.rocketmq.remoting.ClientConfig;
import org.apache.rocketmq.remoting.ServerConfig;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Spy;

import static org.assertj.core.api.Assertions.assertThat;

public class SnodeControllerTest {

    @Spy
    private ServerConfig serverConfig = new ServerConfig();
    @Spy
    private ClientConfig clientConfig = new ClientConfig();
    @Spy
    private ServerConfig mqttServerConfig = new ServerConfig();
    @Spy
    private ClientConfig mqttClientConfig = new ClientConfig();

    private SnodeController snodeController;

    @Before
    public void init() throws CloneNotSupportedException {
        SnodeConfig snodeConfig = new SnodeConfig();
        snodeConfig.setNettyClientConfig(clientConfig);
        serverConfig.setListenPort(10912);
        snodeConfig.setNettyServerConfig(serverConfig);

        MqttConfig mqttConfig = new MqttConfig();
        mqttServerConfig.setListenPort(mqttConfig.getListenPort());
        mqttConfig.setMqttClientConfig(mqttClientConfig);
        mqttConfig.setMqttServerConfig(mqttServerConfig);

        snodeController = new SnodeController(snodeConfig, mqttConfig);
    }

    @Test
    public void testSnodeRestart() {
        assertThat(snodeController.initialize());
        snodeController.start();
        snodeController.shutdown();
    }

    @Test
    public void testSnodeRestartWithAclEnable() throws CloneNotSupportedException {
        System.setProperty("rocketmq.home.dir", "src/test/resources");
        System.setProperty("rocketmq.acl.plain.file", "/conf/plain_acl.yml");
        SnodeConfig snodeConfig = new SnodeConfig();
        snodeConfig.setAclEnable(true);
        assertThat(snodeController.initialize());
        snodeController.start();
        snodeController.shutdown();
    }

}
