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
package org.apache.rocketmq.tools.command.cluster;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.remoting.protocol.body.ClusterInfo;
import org.apache.rocketmq.remoting.protocol.route.BrokerData;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.apache.rocketmq.tools.command.SubCommandException;
import org.apache.rocketmq.tools.command.server.ServerResponseMocker;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ClusterSendMsgRTCommandTest {

    private ServerResponseMocker nameServerMocker;

    @Before
    public void before() {
        nameServerMocker = startNameServer();
        System.setProperty(MixAll.NAMESRV_ADDR_PROPERTY, "localhost:" + nameServerMocker.listenPort());
    }

    @After
    public void after() {
        System.clearProperty(MixAll.NAMESRV_ADDR_PROPERTY);
        nameServerMocker.shutdown();
    }

    @Test
    public void testAmountBelowTwoIsRejected() {
        ClusterSendMsgRTCommand cmd = new ClusterSendMsgRTCommand();
        Options options = ServerUtil.buildCommandlineOptions(new Options());
        String[] subargs = ("-c mockCluster -a 1 -p true -n localhost:" + nameServerMocker.listenPort()).split(" ");
        final CommandLine commandLine =
            ServerUtil.parseCmdLine("mqadmin " + cmd.commandName(), subargs,
                cmd.buildCommandlineOptions(options),
                new DefaultParser());
        try {
            cmd.execute(commandLine, options, null);
            Assert.fail("expected SubCommandException for -a 1");
        } catch (SubCommandException e) {
            Throwable cause = e;
            StringBuilder messages = new StringBuilder();
            while (cause != null) {
                messages.append(cause.getMessage()).append(" ; ");
                cause = cause.getCause();
            }
            Assert.assertTrue(messages.toString(),
                messages.toString().contains("amount must be >= 2"));
        }
    }

    private ServerResponseMocker startNameServer() {
        ClusterInfo clusterInfo = new ClusterInfo();

        HashMap<String, BrokerData> brokerAddressTable = new HashMap<>();
        BrokerData brokerData = new BrokerData();
        brokerData.setBrokerName("mockBrokerName");
        HashMap<Long, String> brokerAddress = new HashMap<>();
        brokerAddress.put(0L, "127.0.0.1:1");
        brokerData.setBrokerAddrs(brokerAddress);
        brokerData.setCluster("mockCluster");
        brokerAddressTable.put("mockBrokerName", brokerData);
        clusterInfo.setBrokerAddrTable(brokerAddressTable);

        HashMap<String, Set<String>> clusterAddressTable = new HashMap<>();
        Set<String> brokerNames = new HashSet<>();
        brokerNames.add("mockBrokerName");
        clusterAddressTable.put("mockCluster", brokerNames);
        clusterInfo.setClusterAddrTable(clusterAddressTable);

        return ServerResponseMocker.startServer(clusterInfo.encode());
    }
}
