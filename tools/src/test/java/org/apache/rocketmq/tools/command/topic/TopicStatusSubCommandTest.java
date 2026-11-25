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
package org.apache.rocketmq.tools.command.topic;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;
import org.apache.rocketmq.remoting.protocol.admin.TopicStatsTable;
import org.apache.rocketmq.remoting.protocol.route.BrokerData;
import org.apache.rocketmq.remoting.protocol.route.TopicRouteData;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.apache.rocketmq.tools.command.server.ServerResponseMocker;
import org.junit.After;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class TopicStatusSubCommandTest {
    private final List<ServerResponseMocker> servers = new ArrayList<>();

    @After
    public void shutdownServers() {
        servers.forEach(ServerResponseMocker::shutdown);
    }

    @Test
    public void testExecute() {
        TopicStatusSubCommand cmd = new TopicStatusSubCommand();
        Options options = ServerUtil.buildCommandlineOptions(new Options());
        String[] subargs = new String[] {"-t unit-test"};
        final CommandLine commandLine =
            ServerUtil.parseCmdLine("mqadmin " + cmd.commandName(), subargs,
                cmd.buildCommandlineOptions(options), new DefaultParser());
        assertThat(commandLine.getOptionValue('t').trim()).isEqualTo("unit-test");
    }

    @Test
    public void testClusterPrintsTotalPutTpsWithTwoDecimalPlaces() throws Exception {
        TopicRouteData route = new TopicRouteData();
        route.getBrokerDatas().add(startBroker("broker-a", 2.0));
        route.getBrokerDatas().add(startBroker("broker-b", 1.5));

        TopicStatusSubCommand cmd = new TopicStatusSubCommand();
        Options options = cmd.buildCommandlineOptions(new Options());
        CommandLine args = new DefaultParser().parse(options, new String[] {"-t", "unit-test", "-c", "test-cluster"});
        String previousNamesrv = System.setProperty(MixAll.NAMESRV_ADDR_PROPERTY, startServer(route));
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        PrintStream previousOut = System.out;
        try (PrintStream captured = new PrintStream(output)) {
            System.setOut(captured);
            cmd.execute(args, options, null);
        } finally {
            System.setOut(previousOut);
            if (previousNamesrv == null) {
                System.clearProperty(MixAll.NAMESRV_ADDR_PROPERTY);
            } else {
                System.setProperty(MixAll.NAMESRV_ADDR_PROPERTY, previousNamesrv);
            }
        }
        assertThat(output.toString().split("\\R")).contains("Topic Put TPS: 3.50");
    }

    private BrokerData startBroker(String name, double putTps) {
        TopicStatsTable stats = new TopicStatsTable();
        stats.setTopicPutTps(putTps);
        HashMap<Long, String> addresses = new HashMap<>();
        addresses.put(MixAll.MASTER_ID, startServer(stats));
        return new BrokerData("test-cluster", name, addresses);
    }

    private String startServer(RemotingSerializable response) {
        ServerResponseMocker server = ServerResponseMocker.startServer(response.encode());
        servers.add(server);
        return "127.0.0.1:" + server.listenPort();
    }
}
