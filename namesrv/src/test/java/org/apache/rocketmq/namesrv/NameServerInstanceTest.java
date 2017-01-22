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
package org.apache.rocketmq.namesrv;

import org.apache.rocketmq.common.namesrv.NamesrvConfig;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.junit.After;
import org.junit.Before;

import static org.assertj.core.api.Assertions.assertThat;

public class NameServerInstanceTest {
    protected NamesrvController nameSrvController = null;
    protected NettyServerConfig nettyServerConfig = new NettyServerConfig();
    protected NamesrvConfig namesrvConfig = new NamesrvConfig();

    @Before
    public void startup() throws Exception {
        nettyServerConfig.setListenPort(9876);
        nameSrvController = new NamesrvController(namesrvConfig, nettyServerConfig);
        boolean initResult = nameSrvController.initialize();
        assertThat(initResult).isTrue();
        nameSrvController.start();
    }

    @After
    public void shutdown() throws Exception {
        if (nameSrvController != null) {
            nameSrvController.shutdown();
        }
        //maybe need to clean the file store. But we do not suggest deleting anything.
    }
}
