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

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Properties;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.protocol.body.ClusterInfo;
import org.apache.rocketmq.remoting.protocol.body.KVTable;
import org.apache.rocketmq.remoting.protocol.body.SubscriptionGroupWrapper;
import org.apache.rocketmq.remoting.protocol.body.TopicConfigSerializeWrapper;
import org.apache.rocketmq.remoting.protocol.route.BrokerData;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ExportMetricsCommandTest {
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void exportsEachBrokersReportedVersion() throws Exception {
        DefaultMQAdminExt admin = mock(DefaultMQAdminExt.class);
        ClusterInfo cluster = new ClusterInfo();
        cluster.setBrokerAddrTable(new HashMap<>());
        addBroker(admin, cluster, "broker-a", "localhost:10911", "V5_3_2");
        addBroker(admin, cluster, "broker-b", "localhost:10912", "V5_4_0");
        addBroker(admin, cluster, "broker-c", "localhost:10913", null);
        cluster.setClusterAddrTable(Collections.singletonMap("cluster", new HashSet<>(cluster.getBrokerAddrTable().keySet())));
        when(admin.examineBrokerClusterInfo()).thenReturn(cluster);

        Path directory = temporaryFolder.getRoot().toPath();
        ExportMetricsCommand command = new ExportMetricsCommand() {
            @Override
            DefaultMQAdminExt createAdmin(RPCHook rpcHook) {
                return admin;
            }
        };
        Options options = command.buildCommandlineOptions(new Options());
        CommandLine args = new DefaultParser().parse(options, new String[] {"-c", "cluster", "-f", directory.toString()});
        command.execute(args, options, null);

        JSONObject report = JSON.parseObject(Files.readAllBytes(directory.resolve("metrics.json"))).getJSONObject("evaluateReport");
        assertEquals("V5_3_2", report.getJSONObject("broker-a").getJSONObject("runtimeVersion").getString("rocketmqVersion"));
        assertEquals("V5_4_0", report.getJSONObject("broker-b").getJSONObject("runtimeVersion").getString("rocketmqVersion"));
        assertEquals("unknown", report.getJSONObject("broker-c").getJSONObject("runtimeVersion").getString("rocketmqVersion"));
    }

    private void addBroker(DefaultMQAdminExt admin, ClusterInfo cluster, String name, String address, String version)
        throws Exception {
        HashMap<Long, String> addresses = new HashMap<>();
        addresses.put(MixAll.MASTER_ID, address);
        cluster.getBrokerAddrTable().put(name, new BrokerData("cluster", name, addresses));

        KVTable metrics = new KVTable();
        for (String key : Arrays.asList("putTps", "getTransferredTps", "msgPutTotalTodayMorning",
            "msgPutTotalYesterdayMorning", "msgGetTotalTodayMorning", "msgGetTotalYesterdayMorning")) {
            metrics.getTable().put(key, "0");
        }
        if (version != null) {
            metrics.getTable().put("brokerVersionDesc", version);
        }
        when(admin.fetchBrokerRuntimeStats(address)).thenReturn(metrics);
        when(admin.getBrokerConfig(address)).thenReturn(new Properties());
        when(admin.getUserSubscriptionGroup(address, 10000)).thenReturn(new SubscriptionGroupWrapper());
        when(admin.getUserTopicConfig(address, false, 10000)).thenReturn(new TopicConfigSerializeWrapper());
    }
}
