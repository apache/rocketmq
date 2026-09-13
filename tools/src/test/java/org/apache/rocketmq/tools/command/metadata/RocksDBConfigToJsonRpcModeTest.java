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
package org.apache.rocketmq.tools.command.metadata;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.remoting.protocol.body.ClusterInfo;
import org.apache.rocketmq.remoting.protocol.route.BrokerData;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.apache.rocketmq.tools.command.SubCommandException;
import org.apache.rocketmq.tools.command.server.ServerResponseMocker;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class RocksDBConfigToJsonRpcModeTest {

    private ServerResponseMocker brokerMocker;

    private ServerResponseMocker nameServerMocker;

    private final PrintStream stdout = System.out;

    private final ByteArrayOutputStream output = new ByteArrayOutputStream();

    @Before
    public void before() {
        System.setOut(new PrintStream(output));
        brokerMocker = ServerResponseMocker.startServer(new byte[0]);
    }

    @After
    public void after() {
        nameServerMocker.shutdown();
        brokerMocker.shutdown();
        System.setOut(stdout);
    }

    @Test
    public void testRpcModeOnlyExportsBrokersOfSpecifiedCluster() throws SubCommandException {
        // brokerB belongs to clusterB and points to a closed port; the export must
        // only be sent to the masters of the requested clusterA
        ClusterInfo clusterInfo = new ClusterInfo();

        HashMap<String, BrokerData> brokerAddressTable = new HashMap<>();
        BrokerData brokerA = new BrokerData();
        brokerA.setBrokerName("brokerA");
        HashMap<Long, String> brokerAAddress = new HashMap<>();
        brokerAAddress.put(0L, "127.0.0.1:" + brokerMocker.listenPort());
        brokerA.setBrokerAddrs(brokerAAddress);
        brokerA.setCluster("clusterA");
        brokerAddressTable.put("brokerA", brokerA);

        BrokerData brokerB = new BrokerData();
        brokerB.setBrokerName("brokerB");
        HashMap<Long, String> brokerBAddress = new HashMap<>();
        brokerBAddress.put(0L, "127.0.0.1:1");
        brokerB.setBrokerAddrs(brokerBAddress);
        brokerB.setCluster("clusterB");
        brokerAddressTable.put("brokerB", brokerB);
        clusterInfo.setBrokerAddrTable(brokerAddressTable);

        HashMap<String, Set<String>> clusterAddressTable = new HashMap<>();
        Set<String> clusterABrokers = new HashSet<>();
        clusterABrokers.add("brokerA");
        clusterAddressTable.put("clusterA", clusterABrokers);
        Set<String> clusterBBrokers = new HashSet<>();
        clusterBBrokers.add("brokerB");
        clusterAddressTable.put("clusterB", clusterBBrokers);
        clusterInfo.setClusterAddrTable(clusterAddressTable);

        nameServerMocker = ServerResponseMocker.startServer(clusterInfo.encode());

        RocksDBConfigToJsonCommand cmd = new RocksDBConfigToJsonCommand();
        Options options = ServerUtil.buildCommandlineOptions(new Options());
        String[] subargs = ("-c clusterA -t topics -n localhost:" + nameServerMocker.listenPort()).split(" ");
        final CommandLine commandLine =
            ServerUtil.parseCmdLine("mqadmin " + cmd.commandName(), subargs,
                cmd.buildCommandlineOptions(options),
                new DefaultParser());
        cmd.execute(commandLine, options, null);

        String out = output.toString();
        Assert.assertTrue(out, out.contains("broker export done."));
        Assert.assertFalse(out, out.contains("brokerB"));
    }
}
