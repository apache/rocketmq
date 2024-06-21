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
package org.apache.rocketmq.tools.command.namesrv;

import java.util.HashMap;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.apache.rocketmq.tools.command.SubCommandException;
import org.apache.rocketmq.tools.command.server.NameServerMocker;
import org.apache.rocketmq.tools.command.server.ServerResponseMocker;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class AddWritePermSubCommandTest {

    private ServerResponseMocker brokerMocker;

    private ServerResponseMocker nameServerMocker;

    @Before
    public void before() {
        brokerMocker = startOneBroker();
        nameServerMocker = startNameServer();
        System.setProperty(MixAll.NAMESRV_ADDR_PROPERTY, "localhost:" + nameServerMocker.listenPort());
    }

    @After
    public void after() {
        brokerMocker.shutdown();
        nameServerMocker.shutdown();
    }

    @Test
    public void testExecute() throws SubCommandException {
        AddWritePermSubCommand cmd = new AddWritePermSubCommand();
        Options options = ServerUtil.buildCommandlineOptions(new Options());
        String[] subargs = new String[]{"-b default-broker"};
        final CommandLine commandLine =
                ServerUtil.parseCmdLine("mqadmin " + cmd.commandName(), subargs,
                    cmd.buildCommandlineOptions(options), new DefaultParser());
        cmd.execute(commandLine, options, null);
    }

    private ServerResponseMocker startNameServer() {
        HashMap<String, String> extMap = new HashMap<>();
        extMap.put("addTopicCount", "1");
        // start name server
        return NameServerMocker.startByDefaultConf(brokerMocker.listenPort(), extMap);
    }

    private ServerResponseMocker startOneBroker() {
        // start broker
        return ServerResponseMocker.startServer(null);
    }
}
