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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.cli.AlreadySelectedException;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.MissingOptionException;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.TopicFilterType;
import org.apache.rocketmq.common.attribute.TopicMessageType;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;
import org.apache.rocketmq.remoting.protocol.body.ClusterInfo;
import org.apache.rocketmq.remoting.protocol.route.BrokerData;
import org.apache.rocketmq.remoting.protocol.statictopic.TopicConfigAndQueueMapping;
import org.apache.rocketmq.remoting.protocol.statictopic.TopicQueueMappingDetail;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.apache.rocketmq.tools.command.SubCommandException;
import org.apache.rocketmq.tools.command.server.ServerResponseMocker;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class GetTopicConfigSubCommandTest {
    private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
    private final PrintStream originalOut = System.out;
    private final List<ServerResponseMocker> mockers = new ArrayList<>();

    @Before
    public void setUp() {
        outContent.reset();
        System.setOut(new PrintStream(outContent));
    }

    @After
    public void tearDown() {
        for (ServerResponseMocker mocker : mockers) {
            mocker.shutdown();
        }
        mockers.clear();
        System.setOut(originalOut);
        outContent.reset();
    }

    @Test
    public void testCommandNameAndOptions() throws ParseException {
        GetTopicConfigSubCommand command = new GetTopicConfigSubCommand();
        Assert.assertEquals("getTopicConfig", command.commandName());
        Assert.assertTrue(command.commandDesc().contains("topic config"));

        Options options = command.buildCommandlineOptions(new Options());
        Assert.assertTrue(options.hasOption("t"));
        Assert.assertTrue(options.hasOption("b"));
        Assert.assertTrue(options.hasOption("c"));

        DefaultParser parser = new DefaultParser();
        CommandLine brokerLine = parser.parse(options,
            new String[] {"-t", "TopicTest", "-b", "127.0.0.1:10911"});
        Assert.assertEquals("TopicTest", brokerLine.getOptionValue('t'));
        Assert.assertEquals("127.0.0.1:10911", brokerLine.getOptionValue('b'));
        Assert.assertFalse(brokerLine.hasOption('c'));

        CommandLine clusterLine = parser.parse(options,
            new String[] {"--topic=TopicTest", "--clusterName=DefaultCluster"});
        Assert.assertTrue(clusterLine.hasOption('c'));
        Assert.assertFalse(clusterLine.hasOption('b'));
        Assert.assertEquals("DefaultCluster", clusterLine.getOptionValue('c'));
    }

    @Test
    public void testTopicIsRequired() {
        GetTopicConfigSubCommand command = new GetTopicConfigSubCommand();
        Options options = command.buildCommandlineOptions(new Options());
        try {
            new DefaultParser().parse(options, new String[] {"-b", "127.0.0.1:10911"});
            Assert.fail("The topic option should be required");
        } catch (ParseException expected) {
            // Commons CLI reports the missing option by its short key ("t") rather than the
            // user-facing "-t" spelling, so the exception type and payload are the stable checks.
            Assert.assertTrue(expected instanceof MissingOptionException);
            Assert.assertTrue(((MissingOptionException) expected).getMissingOptions().contains("t"));
        }
    }

    @Test
    public void testBrokerAndClusterAreMutuallyExclusive() {
        GetTopicConfigSubCommand command = new GetTopicConfigSubCommand();
        Options options = command.buildCommandlineOptions(new Options());
        try {
            new DefaultParser().parse(options, new String[] {"-t", "TopicTest", "-b",
                "127.0.0.1:10911", "-c", "DefaultCluster"});
            Assert.fail("Only one broker target should be accepted");
        } catch (ParseException expected) {
            Assert.assertTrue(expected instanceof AlreadySelectedException);
        }
    }

    @Test
    public void testExecuteWithBrokerAddressPrintsAllConfigurationFields()
        throws SubCommandException {
        TopicConfig config = newConfig("TopicTest", 8, 4, PermName.PERM_READ | PermName.PERM_WRITE);
        config.setTopicMessageType(TopicMessageType.LITE);
        config.setLiteTopicExpiration(30);
        ServerResponseMocker broker = startBroker("broker-a", config);

        String output = execute("-t", "TopicTest", "-b", "127.0.0.1:" + broker.listenPort());

        Assert.assertTrue(output.contains("============127.0.0.1:" + broker.listenPort()
            + "============"));
        assertField(output, "topicName", "TopicTest");
        assertField(output, "readQueueNums", "8");
        assertField(output, "writeQueueNums", "4");
        assertField(output, "perm", "RW- (6)");
        assertField(output, "topicFilterType", "MULTI_TAG");
        assertField(output, "topicSysFlag", "3");
        assertField(output, "order", "true");
        Assert.assertTrue(output.contains("message.type=LITE"));
        Assert.assertTrue(output.contains("lite.topic.expiration=30"));
        Assert.assertTrue(output.contains("mappingDetail"));
        Assert.assertTrue(output.contains("epoch=7"));
    }

    @Test
    public void testExecuteWithClusterQueriesEveryBroker() throws SubCommandException {
        ServerResponseMocker brokerA = startBroker("broker-a", newConfig("TopicTest", 8, 4,
            PermName.PERM_READ | PermName.PERM_WRITE));
        ServerResponseMocker brokerB = startBroker("broker-b", newConfig("TopicTest", 16, 2,
            PermName.PERM_WRITE));

        Map<String, Integer> brokerPorts = new HashMap<>();
        brokerPorts.put("broker-a", brokerA.listenPort());
        brokerPorts.put("broker-b", brokerB.listenPort());
        ServerResponseMocker nameServer = startNameServer("DefaultCluster", brokerPorts);

        String output = execute("-t", "TopicTest", "-c", "DefaultCluster", "-n",
            "127.0.0.1:" + nameServer.listenPort());

        Assert.assertTrue(output.contains("Master: 127.0.0.1:" + brokerA.listenPort()));
        Assert.assertTrue(output.contains("Master: 127.0.0.1:" + brokerB.listenPort()));
        assertField(output, "readQueueNums", "8");
        assertField(output, "readQueueNums", "16");
        assertField(output, "writeQueueNums", "4");
        assertField(output, "writeQueueNums", "2");
    }

    @Test
    public void testExecuteWithoutTargetEnumeratesAllBrokers() throws SubCommandException {
        ServerResponseMocker brokerA = startBroker("broker-a", newConfig("TopicTest", 8, 4,
            PermName.PERM_READ | PermName.PERM_WRITE));
        ServerResponseMocker brokerB = startBroker("broker-b", newConfig("TopicTest", 16, 2,
            PermName.PERM_READ));

        Map<String, Integer> brokerPorts = new HashMap<>();
        brokerPorts.put("broker-a", brokerA.listenPort());
        brokerPorts.put("broker-b", brokerB.listenPort());
        ServerResponseMocker nameServer = startNameServer("DefaultCluster", brokerPorts);

        String output = execute("-t", "TopicTest", "-n",
            "127.0.0.1:" + nameServer.listenPort());

        Assert.assertTrue(output.contains("broker-a:127.0.0.1:" + brokerA.listenPort()));
        Assert.assertTrue(output.contains("broker-b:127.0.0.1:" + brokerB.listenPort()));
        assertField(output, "readQueueNums", "8");
        assertField(output, "readQueueNums", "16");
    }

    @Test
    public void testBrokerWithoutTopicConfigIsSkipped() throws SubCommandException {
        ServerResponseMocker broker = startBroker("broker-a", null);

        String output = execute("-t", "MissingTopic", "-b",
            "127.0.0.1:" + broker.listenPort());

        Assert.assertFalse(output.contains("topicName"));
        Assert.assertFalse(output.contains("readQueueNums"));
        Assert.assertFalse(output.contains("============"));
    }

    private String execute(String... subargs) throws SubCommandException {
        GetTopicConfigSubCommand command = new GetTopicConfigSubCommand();
        Options options = ServerUtil.buildCommandlineOptions(new Options());
        CommandLine commandLine = ServerUtil.parseCmdLine("mqadmin " + command.commandName(),
            subargs, command.buildCommandlineOptions(options), new DefaultParser());
        command.execute(commandLine, options, null);
        return outContent.toString();
    }

    private TopicConfig newConfig(String topic, int readQueueNums, int writeQueueNums, int perm) {
        TopicConfig config = new TopicConfig(topic, readQueueNums, writeQueueNums, perm);
        config.setTopicFilterType(TopicFilterType.MULTI_TAG);
        config.setTopicSysFlag(3);
        config.setOrder(true);
        return config;
    }

    private ServerResponseMocker startBroker(String brokerName, TopicConfig config) {
        byte[] body = null;
        if (config != null) {
            TopicQueueMappingDetail mappingDetail = new TopicQueueMappingDetail(
                config.getTopicName(), config.getReadQueueNums(), brokerName, 7L);
            body = RemotingSerializable.encode(new TopicConfigAndQueueMapping(config, mappingDetail));
        }

        ServerResponseMocker mocker = ServerResponseMocker.startServer(body);
        mockers.add(mocker);
        return mocker;
    }

    private ServerResponseMocker startNameServer(String clusterName,
        Map<String, Integer> brokerPorts) {
        ClusterInfo clusterInfo = new ClusterInfo();
        HashMap<String, BrokerData> brokerAddrTable = new HashMap<>();
        Set<String> brokerNames = new HashSet<>();

        for (Map.Entry<String, Integer> entry : brokerPorts.entrySet()) {
            HashMap<Long, String> brokerAddrs = new HashMap<>();
            brokerAddrs.put(MixAll.MASTER_ID, "127.0.0.1:" + entry.getValue());

            BrokerData brokerData = new BrokerData();
            brokerData.setBrokerName(entry.getKey());
            brokerData.setCluster(clusterName);
            brokerData.setBrokerAddrs(brokerAddrs);
            brokerAddrTable.put(entry.getKey(), brokerData);
            brokerNames.add(entry.getKey());
        }

        HashMap<String, Set<String>> clusterAddrTable = new HashMap<>();
        clusterAddrTable.put(clusterName, brokerNames);
        clusterInfo.setBrokerAddrTable(brokerAddrTable);
        clusterInfo.setClusterAddrTable(clusterAddrTable);

        ServerResponseMocker mocker = ServerResponseMocker.startServer(clusterInfo.encode());
        mockers.add(mocker);
        return mocker;
    }

    private void assertField(String output, String name, String expected) {
        String expectedLine = String.format("%-40s=  %s", name, expected);
        Assert.assertTrue("Expected field line [" + expectedLine + "] in output:\n" + output,
            output.contains(expectedLine));
    }
}
