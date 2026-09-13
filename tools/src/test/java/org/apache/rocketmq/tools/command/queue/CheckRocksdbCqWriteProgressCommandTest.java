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
package org.apache.rocketmq.tools.command.queue;

import com.alibaba.fastjson2.JSON;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.common.CheckRocksdbCqWriteResult;
import org.apache.rocketmq.remoting.protocol.body.ClusterInfo;
import org.apache.rocketmq.remoting.protocol.route.BrokerData;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.apache.rocketmq.tools.command.SubCommandException;
import org.apache.rocketmq.tools.command.server.ServerResponseMocker;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class CheckRocksdbCqWriteProgressCommandTest {

    private ServerResponseMocker brokerMocker;

    private ServerResponseMocker nameServerMocker;

    private final PrintStream stdout = System.out;

    private final ByteArrayOutputStream output = new ByteArrayOutputStream();

    @Before
    public void before() {
        System.setOut(new PrintStream(output));
        brokerMocker = ServerResponseMocker.startServer(JSON.toJSONBytes(new CheckRocksdbCqWriteResult()));
    }

    @After
    public void after() {
        nameServerMocker.shutdown();
        brokerMocker.shutdown();
        System.setOut(stdout);
    }

    @Test
    public void testCheckOnlySpecifiedCluster() throws SubCommandException {
        // brokerB belongs to clusterB and points to a closed port; a command scoped
        // to clusterA must not touch brokers of other clusters
        ClusterInfoBuilder builder = new ClusterInfoBuilder();
        builder.addBroker("brokerA", "clusterA", true, "127.0.0.1:" + brokerMocker.listenPort());
        builder.addBroker("brokerB", "clusterB", true, "127.0.0.1:1");
        nameServerMocker = ServerResponseMocker.startServer(builder.build().encode());

        executeCommand("-c clusterA");

        String out = output.toString();
        Assert.assertTrue(out, out.contains("brokerA"));
        Assert.assertFalse(out, out.contains("brokerB"));
    }

    @Test
    public void testSkipBrokerWithoutMaster() throws SubCommandException {
        // brokerC only registers a slave address; it must be skipped instead of
        // sending the check request through the name server channel
        ClusterInfoBuilder builder = new ClusterInfoBuilder();
        builder.addBroker("brokerA", "clusterA", true, "127.0.0.1:" + brokerMocker.listenPort());
        builder.addBroker("brokerC", "clusterA", false, "127.0.0.1:" + brokerMocker.listenPort());
        nameServerMocker = ServerResponseMocker.startServer(builder.build().encode());

        executeCommand("-c clusterA");

        String out = output.toString();
        Assert.assertTrue(out, out.contains("brokerC has no master"));
        Assert.assertFalse(out, out.contains("brokerC check"));
    }

    private void executeCommand(String args) throws SubCommandException {
        CheckRocksdbCqWriteProgressCommand cmd = new CheckRocksdbCqWriteProgressCommand();
        Options options = ServerUtil.buildCommandlineOptions(new Options());
        String[] subargs = (args + String.format(" -n localhost:%d", nameServerMocker.listenPort())).split(" ");
        final CommandLine commandLine =
            ServerUtil.parseCmdLine("mqadmin " + cmd.commandName(), subargs,
                cmd.buildCommandlineOptions(options),
                new DefaultParser());
        cmd.execute(commandLine, options, null);
    }

    private static class ClusterInfoBuilder {
        private final HashMap<String, BrokerData> brokerAddrTable = new HashMap<>();
        private final HashMap<String, Set<String>> clusterAddrTable = new HashMap<>();

        void addBroker(String brokerName, String clusterName, boolean withMaster, String addr) {
            BrokerData brokerData = new BrokerData();
            brokerData.setBrokerName(brokerName);
            HashMap<Long, String> brokerAddrs = new HashMap<>();
            if (withMaster) {
                brokerAddrs.put(0L, addr);
            } else {
                brokerAddrs.put(1L, addr);
            }
            brokerData.setBrokerAddrs(brokerAddrs);
            brokerData.setCluster(clusterName);
            brokerAddrTable.put(brokerName, brokerData);
            clusterAddrTable.computeIfAbsent(clusterName, k -> new HashSet<>()).add(brokerName);
        }

        ClusterInfo build() {
            ClusterInfo clusterInfo = new ClusterInfo();
            clusterInfo.setBrokerAddrTable(brokerAddrTable);
            clusterInfo.setClusterAddrTable(clusterAddrTable);
            return clusterInfo;
        }
    }
}
