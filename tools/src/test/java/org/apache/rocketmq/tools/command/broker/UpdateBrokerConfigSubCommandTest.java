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
package org.apache.rocketmq.tools.command.broker;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.remoting.protocol.body.ClusterInfo;
import org.apache.rocketmq.remoting.protocol.route.BrokerData;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.apache.rocketmq.tools.command.SubCommandException;
import org.apache.rocketmq.tools.command.server.ServerResponseMocker;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class UpdateBrokerConfigSubCommandTest extends ServerResponseMocker {

    @Override
    protected byte[] getBody() {
        return null;
    }

    @Test
    public void testExecute() throws SubCommandException {
        UpdateBrokerConfigSubCommand cmd = new UpdateBrokerConfigSubCommand();
        Options options = ServerUtil.buildCommandlineOptions(new Options());
        String[] subargs = new String[] {"-b 127.0.0.1:" + listenPort(), "-c default-cluster", "-k topicname", "-v unit_test"};
        final CommandLine commandLine =
            ServerUtil.parseCmdLine("mqadmin " + cmd.commandName(), subargs,
                cmd.buildCommandlineOptions(options), new DefaultParser());
        cmd.execute(commandLine, options, null);
    }

    @Test
    public void testUpdateAllBrokerTrueUpdatesSlaves() throws Exception {
        assertBrokerFanOut("true", 1);
    }

    @Test
    public void testUpdateAllBrokerFalseSkipsSlaves() throws Exception {
        // the option takes the boolean as its argument (mqadmin help: -a,--updateAllBroker <arg>),
        // so an explicit false must leave the slaves alone
        assertBrokerFanOut("false", 0);
    }

    /**
     * Runs the command for a cluster whose broker has a master and a slave, and asserts how many
     * update requests each of them received.
     */
    private void assertBrokerFanOut(String updateAllBroker, int expectedSlaveRequests) throws Exception {
        AtomicInteger masterRequests = new AtomicInteger();
        AtomicInteger slaveRequests = new AtomicInteger();
        ServerResponseMocker master = countingBroker(masterRequests);
        ServerResponseMocker slave = countingBroker(slaveRequests);
        ServerResponseMocker nameServer = startNameServer(master.listenPort(), slave.listenPort());
        // the -n option is only applied by the mqadmin entry point, so the client uses the property
        System.setProperty(MixAll.NAMESRV_ADDR_PROPERTY, "127.0.0.1:" + nameServer.listenPort());
        try {
            UpdateBrokerConfigSubCommand cmd = new UpdateBrokerConfigSubCommand();
            Options options = ServerUtil.buildCommandlineOptions(new Options());
            String[] subargs = new String[] {"-c", "default-cluster", "-a", updateAllBroker, "-k", "key", "-v", "value",
                "-n", "127.0.0.1:" + nameServer.listenPort()};
            final CommandLine commandLine =
                ServerUtil.parseCmdLine("mqadmin " + cmd.commandName(), subargs,
                    cmd.buildCommandlineOptions(options), new DefaultParser());
            cmd.execute(commandLine, options, null);

            assertEquals("master must always be updated", 1, masterRequests.get());
            assertEquals("slave update count", expectedSlaveRequests, slaveRequests.get());
        } finally {
            System.clearProperty(MixAll.NAMESRV_ADDR_PROPERTY);
            nameServer.shutdown();
            slave.shutdown();
            master.shutdown();
        }
    }

    /** A broker that answers every request successfully and counts the requests it received. */
    private ServerResponseMocker countingBroker(AtomicInteger counter) {
        ServerResponseMocker broker = new ServerResponseMocker() {
            @Override
            protected byte[] getBody() {
                counter.incrementAndGet();
                return null;
            }
        };
        broker.start();
        return broker;
    }

    /** A name server that reports one broker with a master and a slave. */
    private ServerResponseMocker startNameServer(int masterPort, int slavePort) {
        ClusterInfo clusterInfo = new ClusterInfo();
        Map<String, BrokerData> brokerAddrTable = new HashMap<>();
        HashMap<Long, String> brokerAddrs = new HashMap<>();
        brokerAddrs.put(MixAll.MASTER_ID, "127.0.0.1:" + masterPort);
        brokerAddrs.put(1L, "127.0.0.1:" + slavePort);
        brokerAddrTable.put("broker-a", new BrokerData("default-cluster", "broker-a", brokerAddrs));
        clusterInfo.setBrokerAddrTable(brokerAddrTable);
        Map<String, Set<String>> clusterAddrTable = new HashMap<>();
        clusterAddrTable.put("default-cluster", new HashSet<>(Collections.singletonList("broker-a")));
        clusterInfo.setClusterAddrTable(clusterAddrTable);
        return ServerResponseMocker.startServer(clusterInfo.encode());
    }
}
