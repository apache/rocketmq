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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.protocol.body.ClusterInfo;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.SubCommandException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.internal.util.collections.Sets;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Set;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

public class GetMasterSlaveDiffCommandTest {

    private GetMasterSlaveDiffCommand cmd;

    private static DefaultMQAdminExt defaultMQAdminExt;

    private static final String CLUSTER_NAME = "DefaultCluster";

    private static final String BROKER_NAME = "broker-a";

    @Before
    public void before() throws NoSuchFieldException, IllegalAccessException, RemotingException, MQBrokerException,
            InterruptedException, MQClientException {

        ClusterInfo clusterInfo = new ClusterInfo();
        HashMap<String, Set<String>> clusterMap = new HashMap<>(4);
        clusterMap.put(CLUSTER_NAME, Sets.newSet(BROKER_NAME));
        clusterInfo.setClusterAddrTable(clusterMap);

        HashMap<Long, String> brokerMap = new HashMap<>(4);
        brokerMap.put(0L, "127.0.0.1:10911");
        BrokerData brokerData = new BrokerData(CLUSTER_NAME, BROKER_NAME, brokerMap);
        HashMap<String, BrokerData> brokerAddrTable = new HashMap<>();
        brokerAddrTable.put(BROKER_NAME, brokerData);
        clusterInfo.setBrokerAddrTable(brokerAddrTable);

        defaultMQAdminExt = new DefaultMQAdminExt();
        defaultMQAdminExt = spy(defaultMQAdminExt);

        cmd = new GetMasterSlaveDiffCommand();
        Field field = cmd.getClass().getDeclaredField("defaultMQAdminExt");
        field.setAccessible(true);
        field.set(cmd, defaultMQAdminExt);

        doReturn(clusterInfo).when(defaultMQAdminExt).examineBrokerClusterInfo();
        doReturn(1024L).when(defaultMQAdminExt).getMasterSlaveDiff(anyString());
    }

    @After
    public void after() {}

    @Test
    public void testExecute() throws SubCommandException, RemotingException, MQBrokerException, InterruptedException, MQClientException {
        PrintStream out = System.out;
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        System.setOut(new PrintStream(bos));
        Options options = ServerUtil.buildCommandlineOptions(new Options());
        String[] subargs = new String[] {"-b " + BROKER_NAME , "-c " + CLUSTER_NAME};
        final CommandLine commandLine =
            ServerUtil.parseCmdLine("mqadmin " + cmd.commandName(), subargs, cmd.buildCommandlineOptions(options), new PosixParser());
        cmd.execute(commandLine, options, null);
        String s = new String(bos.toByteArray());
        System.setOut(out);
        Assert.assertTrue(s.contains("#Diff(Bytes)") && s.contains("1024"));
    }
}
