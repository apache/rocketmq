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
package org.apache.rocketmq.tools.command.export;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
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

public class ExportMetricsCommandTest {

    private ServerResponseMocker nameServerMocker;

    private final PrintStream stdout = System.out;

    private final ByteArrayOutputStream output = new ByteArrayOutputStream();

    @Before
    public void before() {
        System.setOut(new PrintStream(output));
        nameServerMocker = startNameServer();
        System.setProperty(MixAll.NAMESRV_ADDR_PROPERTY, "localhost:" + nameServerMocker.listenPort());
    }

    @After
    public void after() {
        System.clearProperty(MixAll.NAMESRV_ADDR_PROPERTY);
        nameServerMocker.shutdown();
        System.setOut(stdout);
    }

    @Test
    public void testUnknownClusterReportsCleanError() throws SubCommandException {
        ExportMetricsCommand cmd = new ExportMetricsCommand();
        Options options = ServerUtil.buildCommandlineOptions(new Options());
        String[] subargs = "-c clusterTypo -f /tmp/rocketmq_export_metrics_test".split(" ");
        final CommandLine commandLine =
            ServerUtil.parseCmdLine("mqadmin " + cmd.commandName(), subargs,
                cmd.buildCommandlineOptions(options),
                new DefaultParser());
        // must not throw: a typo in the cluster name should print a clean error
        cmd.execute(commandLine, options, null);
        Assert.assertTrue(output.toString(), output.toString().contains("cluster [clusterTypo] not exist"));
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
