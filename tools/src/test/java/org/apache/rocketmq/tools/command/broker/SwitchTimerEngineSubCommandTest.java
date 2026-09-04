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
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.remoting.protocol.body.ClusterInfo;
import org.apache.rocketmq.remoting.protocol.route.BrokerData;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.apache.rocketmq.tools.command.SubCommandException;
import org.apache.rocketmq.tools.command.server.ServerResponseMocker;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class SwitchTimerEngineSubCommandTest {

    private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
    private final PrintStream originalOut = System.out;

    @Before
    public void setUp() throws Exception {
        outContent.reset();
        System.setOut(new PrintStream(outContent));
    }

    @After
    public void tearDown() throws Exception {
        System.setOut(originalOut);
        outContent.reset();
    }

    @Test
    public void testCommandName() {
        SwitchTimerEngineSubCommand cmd = new SwitchTimerEngineSubCommand();
        Assert.assertEquals("switchTimerEngine", cmd.commandName());
    }

    @Test
    public void testCommandDesc() {
        SwitchTimerEngineSubCommand cmd = new SwitchTimerEngineSubCommand();
        Assert.assertEquals("switch the engine of timer message in broker", cmd.commandDesc());
    }

    @Test
    public void testBuildCommandlineOptions() {
        SwitchTimerEngineSubCommand cmd = new SwitchTimerEngineSubCommand();
        Options options = cmd.buildCommandlineOptions(new Options());
        Assert.assertNotNull(options);
        Assert.assertTrue(options.hasOption("b"));
        Assert.assertTrue(options.hasOption("c"));
        Assert.assertTrue(options.hasOption("e"));
    }

    @Test
    public void testExecuteWithInvalidEngineType() throws SubCommandException {
        SwitchTimerEngineSubCommand cmd = new SwitchTimerEngineSubCommand();
        Options options = ServerUtil.buildCommandlineOptions(new Options());
        String[] subargs = new String[] {
            "-b", "127.0.0.1:10911",
            "-e", "X"
        };
        final CommandLine commandLine = ServerUtil.parseCmdLine("mqadmin " + cmd.commandName(), subargs,
            cmd.buildCommandlineOptions(options), new DefaultParser());
        cmd.execute(commandLine, options, null);
        String output = outContent.toString();
        Assert.assertTrue(output.contains("switchTimerEngine engineType must be R or F"));
    }

    @Test
    public void testExecuteWithEmptyEngineType() throws SubCommandException {
        SwitchTimerEngineSubCommand cmd = new SwitchTimerEngineSubCommand();
        Options options = ServerUtil.buildCommandlineOptions(new Options());
        String[] subargs = new String[] {
            "-b", "127.0.0.1:10911",
            "-e", ""
        };
        final CommandLine commandLine = ServerUtil.parseCmdLine("mqadmin " + cmd.commandName(), subargs,
            cmd.buildCommandlineOptions(options), new DefaultParser());
        cmd.execute(commandLine, options, null);
        String output = outContent.toString();
        Assert.assertTrue(output.contains("switchTimerEngine engineType must be R or F"));
    }

    @Test
    public void testOptionGroupWithBrokerAddrOnly() throws ParseException {
        // Test that -b option alone is valid
        SwitchTimerEngineSubCommand cmd = new SwitchTimerEngineSubCommand();
        Options options = cmd.buildCommandlineOptions(new Options());
        String[] subargs = new String[] {
            "-b", "127.0.0.1:10911",
            "-e", MessageConst.TIMER_ENGINE_ROCKSDB_TIMELINE
        };
        DefaultParser parser = new DefaultParser();
        CommandLine commandLine = parser.parse(options, subargs);
        Assert.assertTrue(commandLine.hasOption('b'));
        Assert.assertFalse(commandLine.hasOption('c'));
        Assert.assertEquals("127.0.0.1:10911", commandLine.getOptionValue('b'));
    }

    @Test
    public void testOptionGroupWithClusterNameOnly() throws ParseException {
        // Test that -c option alone is valid
        SwitchTimerEngineSubCommand cmd = new SwitchTimerEngineSubCommand();
        Options options = cmd.buildCommandlineOptions(new Options());
        String[] subargs = new String[] {
            "-c", "default-cluster",
            "-e", MessageConst.TIMER_ENGINE_ROCKSDB_TIMELINE
        };
        DefaultParser parser = new DefaultParser();
        CommandLine commandLine = parser.parse(options, subargs);
        Assert.assertFalse(commandLine.hasOption('b'));
        Assert.assertTrue(commandLine.hasOption('c'));
        Assert.assertEquals("default-cluster", commandLine.getOptionValue('c'));
    }

    @Test
    public void testOptionGroupWithNeitherOption() {
        // Test that providing neither -b nor -c should fail (required)
        SwitchTimerEngineSubCommand cmd = new SwitchTimerEngineSubCommand();
        Options options = cmd.buildCommandlineOptions(new Options());
        String[] subargs = new String[] {
            "-e", MessageConst.TIMER_ENGINE_ROCKSDB_TIMELINE
        };
        DefaultParser parser = new DefaultParser();
        try {
            parser.parse(options, subargs);
            Assert.fail("Should throw ParseException when neither -b nor -c is provided");
        } catch (ParseException e) {
            String message = e.getMessage();
            Assert.assertNotNull(message);
            Assert.assertEquals("Missing required option: [-b update which broker, -c update which cluster]", message);
        }
    }

    @Test
    public void testExecuteWithBrokerAddr() throws SubCommandException {
        ServerResponseMocker brokerMocker = null;
        try {
            // Start broker mock server (return SUCCESS for switchTimerEngine)
            brokerMocker = ServerResponseMocker.startServer(null);

            SwitchTimerEngineSubCommand cmd = new SwitchTimerEngineSubCommand();
            Options options = ServerUtil.buildCommandlineOptions(new Options());
            String[] subargs = new String[] {
                "-b", "127.0.0.1:" + brokerMocker.listenPort(),
                "-e", MessageConst.TIMER_ENGINE_ROCKSDB_TIMELINE
            };
            final CommandLine commandLine = ServerUtil.parseCmdLine("mqadmin " + cmd.commandName(), subargs,
                cmd.buildCommandlineOptions(options), new DefaultParser());
            cmd.execute(commandLine, options, null);

            String output = outContent.toString();
            // Verify the success message with engine name and broker address
            Assert.assertTrue(output.contains("switchTimerEngine to ROCKSDB_TIMELINE success, " + "127.0.0.1:" + brokerMocker.listenPort()));
        } finally {
            if (brokerMocker != null) {
                brokerMocker.shutdown();
            }
        }
    }

    @Test
    public void testExecuteWithClusterName() throws SubCommandException {
        ServerResponseMocker brokerMocker = null;
        ServerResponseMocker nameServerMocker = null;
        String originalNamesrvAddr = null;
        String mockNamesrvAddr = null;
        try {
            // Start broker mock server (return SUCCESS for switchTimerEngine)
            brokerMocker = ServerResponseMocker.startServer(null);

            // Start name server mock server (return ClusterInfo for examineBrokerClusterInfo)
            nameServerMocker = startNameServer(brokerMocker.listenPort());
            mockNamesrvAddr = "127.0.0.1:" + nameServerMocker.listenPort();

            originalNamesrvAddr = System.getProperty(MixAll.NAMESRV_ADDR_PROPERTY);
            System.setProperty(MixAll.NAMESRV_ADDR_PROPERTY, mockNamesrvAddr);

            SwitchTimerEngineSubCommand cmd = new SwitchTimerEngineSubCommand();
            Options options = ServerUtil.buildCommandlineOptions(new Options());
            String[] subargs = new String[] {
                "-c", "mockCluster",
                "-e", MessageConst.TIMER_ENGINE_FILE_TIME_WHEEL
            };
            final CommandLine commandLine = ServerUtil.parseCmdLine("mqadmin " + cmd.commandName(), subargs,
                cmd.buildCommandlineOptions(options), new DefaultParser());
            cmd.execute(commandLine, options, null);

            String output = outContent.toString();
            // Verify the success message with engine name and broker address
            Assert.assertTrue(output.contains("switchTimerEngine to FILE_TIME_WHEEL success, " + "127.0.0.1:" + brokerMocker.listenPort()));
        } finally {
            // Restore original system property
            if (originalNamesrvAddr != null) {
                System.setProperty(MixAll.NAMESRV_ADDR_PROPERTY, originalNamesrvAddr);
            } else {
                System.clearProperty(MixAll.NAMESRV_ADDR_PROPERTY);
            }
            if (brokerMocker != null) {
                brokerMocker.shutdown();
            }
            if (nameServerMocker != null) {
                nameServerMocker.shutdown();
            }
        }
    }

    private ServerResponseMocker startNameServer(int brokerPort) {
        ClusterInfo clusterInfo = new ClusterInfo();

        HashMap<String, BrokerData> brokerAddressTable = new HashMap<>();
        BrokerData brokerData = new BrokerData();
        brokerData.setBrokerName("mockBrokerName");
        HashMap<Long, String> brokerAddress = new HashMap<>();
        brokerAddress.put(MixAll.MASTER_ID, "127.0.0.1:" + brokerPort);
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
}

