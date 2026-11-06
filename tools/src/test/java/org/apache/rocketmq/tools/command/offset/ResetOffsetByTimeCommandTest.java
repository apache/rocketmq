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
package org.apache.rocketmq.tools.command.offset;

import java.util.HashMap;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.remoting.protocol.body.ResetOffsetBody;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.SubCommandException;
import org.apache.rocketmq.tools.command.server.NameServerMocker;
import org.apache.rocketmq.tools.command.server.ServerResponseMocker;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class ResetOffsetByTimeCommandTest {

    private ServerResponseMocker brokerMocker;

    private ServerResponseMocker nameServerMocker;

    @Before
    public void before() {
        brokerMocker = startOneBroker();
        nameServerMocker = NameServerMocker.startByDefaultConf(brokerMocker.listenPort());
    }

    @After
    public void after() {
        brokerMocker.shutdown();
        nameServerMocker.shutdown();
    }

    @Test
    public void testExecute() throws SubCommandException {
        ResetOffsetByTimeCommand cmd = new ResetOffsetByTimeCommand();
        Options options = ServerUtil.buildCommandlineOptions(new Options());
        String[] subargs = new String[] {
            "-g default-group", "-t unit-test", "-s 1412131213231", "-f false",
            String.format("-n localhost:%d", nameServerMocker.listenPort())};
        final CommandLine commandLine =
            ServerUtil.parseCmdLine("mqadmin " + cmd.commandName(), subargs,
                cmd.buildCommandlineOptions(options), new DefaultParser());
        cmd.execute(commandLine, options, null);
    }

    @Test
    public void testExecuteWithClusterOptionKeepsJavaClientProtocol() throws Exception {
        DefaultMQAdminExt adminExt = Mockito.mock(DefaultMQAdminExt.class);
        Mockito.when(adminExt.resetOffsetByTimestamp(Mockito.eq("default-cluster"), Mockito.eq("unit-test"),
                Mockito.eq("default-group"), Mockito.anyLong(), Mockito.anyBoolean(), Mockito.eq(false)))
            .thenReturn(new HashMap<>());
        ResetOffsetByTimeCommand cmd = new ResetOffsetByTimeCommand() {
            @Override
            protected DefaultMQAdminExt createDefaultMQAdminExt(RPCHook rpcHook) {
                return adminExt;
            }
        };
        Options options = ServerUtil.buildCommandlineOptions(new Options());
        String[] subargs = new String[] {
            "-g default-group", "-t unit-test", "-s 1412131213231", "-f false", "-c default-cluster"};
        final CommandLine commandLine =
            ServerUtil.parseCmdLine("mqadmin " + cmd.commandName(), subargs,
                cmd.buildCommandlineOptions(options), new DefaultParser());
        cmd.execute(commandLine, options, null);

        Mockito.verify(adminExt).resetOffsetByTimestamp("default-cluster", "unit-test",
            "default-group", 1412131213231L, false, false);
    }

    private ServerResponseMocker startOneBroker() {
        ResetOffsetBody resetOffsetBody = new ResetOffsetBody();
        // start broker
        return ServerResponseMocker.startServer(resetOffsetBody.encode());
    }
}
