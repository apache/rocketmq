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

import org.apache.rocketmq.remoting.ClientConfig;
import org.apache.rocketmq.remoting.ServerConfig;
import org.apache.rocketmq.snode.config.SnodeConfig;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class SnodeControllerTest {

    @Test
    public void testSnodeRestart() {
        ServerConfig serverConfig = new ServerConfig();
        serverConfig.setListenPort(10912);
        SnodeController snodeController = new SnodeController(
            serverConfig,
            new ClientConfig(),
            new SnodeConfig());
        assertThat(snodeController.initialize());
        snodeController.start();
        snodeController.shutdown();
    }

    @Test
    public void testSnodeRestartWithAclEnable() {
        System.setProperty("rocketmq.home.dir", "src/test/resources");
        System.setProperty("rocketmq.acl.plain.file", "/conf/plain_acl.yml");
        SnodeConfig snodeConfig = new SnodeConfig();
        snodeConfig.setAclEnable(true);
        SnodeController snodeController = new SnodeController(
            new ServerConfig(),
            new ClientConfig(),
            snodeConfig);
        assertThat(snodeController.initialize());
        snodeController.start();
        snodeController.shutdown();
    }

}
