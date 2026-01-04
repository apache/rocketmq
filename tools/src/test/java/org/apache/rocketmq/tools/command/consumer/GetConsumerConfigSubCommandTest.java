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
package org.apache.rocketmq.tools.command.consumer;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.remoting.protocol.body.ClusterInfo;
import org.apache.rocketmq.remoting.protocol.body.Connection;
import org.apache.rocketmq.remoting.protocol.body.ConsumerConnection;
import org.apache.rocketmq.remoting.protocol.route.BrokerData;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.apache.rocketmq.tools.command.SubCommandException;
import org.apache.rocketmq.tools.command.server.ServerResponseMocker;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

public class GetConsumerConfigSubCommandTest {

    private ServerResponseMocker brokerMocker;

    private ServerResponseMocker nameServerMocker;

    @Before
    public void before() {
        brokerMocker = startOneBroker();
        nameServerMocker = startNameServer();
    }

    @After
    public void after() {
        brokerMocker.shutdown();
        nameServerMocker.shutdown();
    }

    @Test
    public void testExecute() throws SubCommandException {
        GetConsumerConfigSubCommand cmd = new GetConsumerConfigSubCommand();
        Options options = ServerUtil.buildCommandlineOptions(new Options());
        String[] subargs = new String[] {"-g group_test", String.format("-n localhost:%d", nameServerMocker.listenPort())};
        final CommandLine commandLine =
            ServerUtil.parseCmdLine("mqadmin " + cmd.commandName(), subargs,
                cmd.buildCommandlineOptions(options),
                new DefaultParser());
        cmd.execute(commandLine, options, null);
    }

    private ServerResponseMocker startNameServer() {
        ClusterInfo clusterInfo = new ClusterInfo();

        HashMap<String, BrokerData> brokerAddressTable = new HashMap<>();
        BrokerData brokerData = new BrokerData();
        brokerData.setBrokerName("mockBrokerName");
        HashMap<Long, String> brokerAddress = new HashMap<>();
        brokerAddress.put(1L, "127.0.0.1:" + brokerMocker.listenPort());
        brokerData.setBrokerAddrs(brokerAddress);
        brokerData.setCluster("mockCluster");
        brokerAddressTable.put("mockBrokerName", brokerData);
        clusterInfo.setBrokerAddrTable(brokerAddressTable);

        HashMap<String, Set<String>> clusterAddressTable = new HashMap<>();
        Set<String> brokerNames = new HashSet<>();
        brokerNames.add("mockBrokerName");
        clusterAddressTable.put("mockCluster", brokerNames);
        clusterInfo.setClusterAddrTable(clusterAddressTable);

        // start name server
        return ServerResponseMocker.startServer(clusterInfo.encode());
    }

    private ServerResponseMocker startOneBroker() {
        ConsumerConnection consumerConnection = new ConsumerConnection();
        HashSet<Connection> connectionSet = new HashSet<>();
        Connection connection = new Connection();
        connectionSet.add(connection);
        consumerConnection.setConnectionSet(connectionSet);
        // start broker
        return ServerResponseMocker.startServer(consumerConnection.encode());
    }
}
