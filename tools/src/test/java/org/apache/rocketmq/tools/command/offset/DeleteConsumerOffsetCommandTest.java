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

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.remoting.protocol.body.ClusterInfo;
import org.apache.rocketmq.remoting.protocol.route.BrokerData;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.apache.rocketmq.tools.command.server.ServerResponseMocker;
import org.junit.Assert;
import org.junit.Test;

public class DeleteConsumerOffsetCommandTest {

    @Test
    public void testCommandMetadata() {
        DeleteConsumerOffsetCommand command = new DeleteConsumerOffsetCommand();

        Assert.assertEquals("deleteConsumerOffset", command.commandName());
        Assert.assertEquals("Delete consumer offset for a consumer group and topic.", command.commandDesc());
    }

    @Test
    public void testBuildCommandlineOptions() {
        DeleteConsumerOffsetCommand command = new DeleteConsumerOffsetCommand();
        Options options = command.buildCommandlineOptions(new Options());

        Assert.assertTrue(options.hasOption("g"));
        Assert.assertTrue(options.hasOption("t"));
        Assert.assertTrue(options.hasOption("b"));
        Assert.assertTrue(options.hasOption("c"));
        Assert.assertTrue(options.getOption("g").isRequired());
        Assert.assertTrue(options.getOption("t").isRequired());
    }

    @Test
    public void testBrokerTargetOption() throws ParseException {
        DeleteConsumerOffsetCommand command = new DeleteConsumerOffsetCommand();
        Options options = command.buildCommandlineOptions(new Options());

        CommandLine commandLine = new DefaultParser().parse(options,
            new String[] {"-g", "GroupName", "-t", "TopicName", "-b", "127.0.0.1:10911"});

        Assert.assertTrue(commandLine.hasOption('b'));
        Assert.assertFalse(commandLine.hasOption('c'));
    }

    @Test
    public void testClusterTargetOption() throws ParseException {
        DeleteConsumerOffsetCommand command = new DeleteConsumerOffsetCommand();
        Options options = command.buildCommandlineOptions(new Options());

        CommandLine commandLine = new DefaultParser().parse(options,
            new String[] {"-g", "GroupName", "-t", "TopicName", "-c", "DefaultCluster"});

        Assert.assertFalse(commandLine.hasOption('b'));
        Assert.assertTrue(commandLine.hasOption('c'));
    }

    @Test
    public void testTargetIsRequired() {
        DeleteConsumerOffsetCommand command = new DeleteConsumerOffsetCommand();
        Options options = command.buildCommandlineOptions(new Options());

        try {
            new DefaultParser().parse(options, new String[] {"-g", "GroupName", "-t", "TopicName"});
            Assert.fail("Expected a ParseException when no broker or cluster is specified");
        } catch (ParseException expected) {
            Assert.assertNotNull(expected.getMessage());
        }
    }

    @Test
    public void testTargetsAreMutuallyExclusive() {
        DeleteConsumerOffsetCommand command = new DeleteConsumerOffsetCommand();
        Options options = command.buildCommandlineOptions(new Options());

        try {
            new DefaultParser().parse(options, new String[] {
                "-g", "GroupName", "-t", "TopicName", "-b", "127.0.0.1:10911", "-c", "DefaultCluster"
            });
            Assert.fail("Expected a ParseException when both broker and cluster are specified");
        } catch (ParseException expected) {
            Assert.assertNotNull(expected.getMessage());
        }
    }

    @Test
    public void testExecuteByBrokerAddress() throws Exception {
        ServerResponseMocker brokerMocker = ServerResponseMocker.startServer(new byte[0]);
        PrintStream originalOut = System.out;
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        System.setOut(new PrintStream(output));
        try {
            DeleteConsumerOffsetCommand command = new DeleteConsumerOffsetCommand();
            Options options = ServerUtil.buildCommandlineOptions(new Options());
            String brokerAddress = "127.0.0.1:" + brokerMocker.listenPort();
            CommandLine commandLine = ServerUtil.parseCmdLine("mqadmin " + command.commandName(),
                new String[] {"-g", "GroupName", "-t", "TopicName", "-b", brokerAddress},
                command.buildCommandlineOptions(options), new DefaultParser());

            command.execute(commandLine, options, null);

            Assert.assertTrue(output.toString().contains(
                "delete consumer offset for group [GroupName] and topic [TopicName] from broker ["
                    + brokerAddress + "] success."));
        } finally {
            System.setOut(originalOut);
            brokerMocker.shutdown();
        }
    }

    @Test
    public void testExecuteByClusterName() throws Exception {
        ServerResponseMocker brokerMocker = ServerResponseMocker.startServer(new byte[0]);
        ServerResponseMocker nameServerMocker = startNameServer(brokerMocker.listenPort());
        String originalNamesrvAddr = System.getProperty(MixAll.NAMESRV_ADDR_PROPERTY);
        PrintStream originalOut = System.out;
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        System.setOut(new PrintStream(output));
        System.setProperty(MixAll.NAMESRV_ADDR_PROPERTY, "127.0.0.1:" + nameServerMocker.listenPort());
        try {
            DeleteConsumerOffsetCommand command = new DeleteConsumerOffsetCommand();
            Options options = ServerUtil.buildCommandlineOptions(new Options());
            CommandLine commandLine = ServerUtil.parseCmdLine("mqadmin " + command.commandName(),
                new String[] {"-g", "GroupName", "-t", "TopicName", "-c", "MockCluster"},
                command.buildCommandlineOptions(options), new DefaultParser());

            command.execute(commandLine, options, null);

            Assert.assertTrue(output.toString().contains(
                "delete consumer offset for group [GroupName] and topic [TopicName] from broker [127.0.0.1:"
                    + brokerMocker.listenPort() + "] in cluster [MockCluster] success."));
        } finally {
            if (originalNamesrvAddr == null) {
                System.clearProperty(MixAll.NAMESRV_ADDR_PROPERTY);
            } else {
                System.setProperty(MixAll.NAMESRV_ADDR_PROPERTY, originalNamesrvAddr);
            }
            System.setOut(originalOut);
            nameServerMocker.shutdown();
            brokerMocker.shutdown();
        }
    }

    private ServerResponseMocker startNameServer(int brokerPort) {
        BrokerData brokerData = new BrokerData();
        brokerData.setBrokerName("MockBroker");
        brokerData.setCluster("MockCluster");
        HashMap<Long, String> brokerAddresses = new HashMap<>();
        brokerAddresses.put(MixAll.MASTER_ID, "127.0.0.1:" + brokerPort);
        brokerData.setBrokerAddrs(brokerAddresses);

        ClusterInfo clusterInfo = new ClusterInfo();
        HashMap<String, BrokerData> brokerAddressTable = new HashMap<>();
        brokerAddressTable.put("MockBroker", brokerData);
        clusterInfo.setBrokerAddrTable(brokerAddressTable);
        HashMap<String, Set<String>> clusterAddressTable = new HashMap<>();
        Set<String> brokerNames = new HashSet<>();
        brokerNames.add("MockBroker");
        clusterAddressTable.put("MockCluster", brokerNames);
        clusterInfo.setClusterAddrTable(clusterAddressTable);

        return ServerResponseMocker.startServer(clusterInfo.encode());
    }
}
