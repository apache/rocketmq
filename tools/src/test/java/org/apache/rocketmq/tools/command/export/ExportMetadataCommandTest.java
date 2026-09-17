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

import com.alibaba.fastjson2.JSONObject;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.remoting.protocol.body.ClusterInfo;
import org.apache.rocketmq.remoting.protocol.body.SubscriptionGroupWrapper;
import org.apache.rocketmq.remoting.protocol.route.BrokerData;
import org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.apache.rocketmq.tools.command.server.ServerResponseMocker;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ExportMetadataCommandTest {

    private ServerResponseMocker brokerMockerA;

    private ServerResponseMocker brokerMockerB;

    private ServerResponseMocker nameServerMocker;

    private final PrintStream stdout = System.out;

    private final ByteArrayOutputStream output = new ByteArrayOutputStream();

    private static final String EXPORT_DIR = "/tmp/rocketmq_export_metadata_test";

    @Before
    public void before() {
        System.setOut(new PrintStream(output));
        // two distinct broker addresses so the cluster has two masters to iterate
        brokerMockerA = startBroker();
        brokerMockerB = startBroker();
        nameServerMocker = startNameServer();
        System.setProperty(MixAll.NAMESRV_ADDR_PROPERTY, "localhost:" + nameServerMocker.listenPort());
    }

    @After
    public void after() {
        System.clearProperty(MixAll.NAMESRV_ADDR_PROPERTY);
        nameServerMocker.shutdown();
        brokerMockerA.shutdown();
        brokerMockerB.shutdown();
        System.setOut(stdout);
    }

    @Test
    public void testSubscriptionGroupRetryQueueNumsNotAccumulatedAcrossBrokers() throws Exception {
        ExportMetadataCommand cmd = new ExportMetadataCommand();
        Options options = ServerUtil.buildCommandlineOptions(new Options());
        String[] subargs = ("-c clusterA -g -f " + EXPORT_DIR).split(" ");
        final CommandLine commandLine =
            ServerUtil.parseCmdLine("mqadmin " + cmd.commandName(), subargs,
                cmd.buildCommandlineOptions(options),
                new DefaultParser());
        cmd.execute(commandLine, options, null);

        File exportFile = new File(EXPORT_DIR, "subscriptionGroup.json");
        Assert.assertTrue(exportFile + " should exist", exportFile.exists());
        JSONObject exported = JSONObject.parseObject(new String(Files.readAllBytes(exportFile.toPath()), StandardCharsets.UTF_8));
        JSONObject group = exported.getJSONObject("subscriptionGroupTable").getJSONObject("groupA");
        Assert.assertNotNull("groupA should be exported", group);
        Assert.assertEquals("retryQueueNums must not be summed across the two masters",
            1, group.getIntValue("retryQueueNums"));
    }

    private ServerResponseMocker startBroker() {
        SubscriptionGroupWrapper wrapper = new SubscriptionGroupWrapper();
        SubscriptionGroupConfig config = new SubscriptionGroupConfig();
        config.setGroupName("groupA");
        config.setRetryQueueNums(1);
        wrapper.getSubscriptionGroupTable().put("groupA", config);
        return ServerResponseMocker.startServer(wrapper.encode());
    }

    private ServerResponseMocker startNameServer() {
        ClusterInfo clusterInfo = new ClusterInfo();

        HashMap<String, BrokerData> brokerAddressTable = new HashMap<>();
        HashMap<String, String> masterAddrs = new HashMap<>();
        masterAddrs.put("brokerA", "127.0.0.1:" + brokerMockerA.listenPort());
        masterAddrs.put("brokerB", "127.0.0.1:" + brokerMockerB.listenPort());
        for (String brokerName : new String[] {"brokerA", "brokerB"}) {
            BrokerData brokerData = new BrokerData();
            brokerData.setBrokerName(brokerName);
            HashMap<Long, String> brokerAddress = new HashMap<>();
            brokerAddress.put(0L, masterAddrs.get(brokerName));
            brokerData.setBrokerAddrs(brokerAddress);
            brokerData.setCluster("clusterA");
            brokerAddressTable.put(brokerName, brokerData);
        }
        clusterInfo.setBrokerAddrTable(brokerAddressTable);

        HashMap<String, Set<String>> clusterAddressTable = new HashMap<>();
        Set<String> brokerNames = new HashSet<>();
        brokerNames.add("brokerA");
        brokerNames.add("brokerB");
        clusterAddressTable.put("clusterA", brokerNames);
        clusterInfo.setClusterAddrTable(clusterAddressTable);

        return ServerResponseMocker.startServer(clusterInfo.encode());
    }
}
