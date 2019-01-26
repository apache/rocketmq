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

import com.google.common.collect.Sets;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.protocol.body.ClusterInfo;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.common.protocol.route.QueueData;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.remoting.exception.RemotingConnectException;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.SubCommandException;
import org.assertj.core.util.Lists;
import org.assertj.core.util.Maps;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;


@RunWith(MockitoJUnitRunner.class)
public class UpdateTopicPermSubCommandTest {


    @Spy
    UpdateTopicPermSubCommand cmd = new UpdateTopicPermSubCommand();

    @Mock
    DefaultMQAdminExt mqAdminExt;

    @Before
    public void init() {
        Field field = null;
        try {
            field = UpdateTopicPermSubCommand.class.getDeclaredField("defaultMQAdminExt");
            field.setAccessible(true);
            field.set(cmd, mqAdminExt);
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
        try {
            ClusterInfo info = getClusterInfo();

            when(mqAdminExt.examineBrokerClusterInfo()).thenReturn(info);


        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (RemotingConnectException e) {
            e.printStackTrace();
        } catch (RemotingTimeoutException e) {
            e.printStackTrace();
        } catch (RemotingSendRequestException e) {
            e.printStackTrace();
        } catch (MQBrokerException e) {
            e.printStackTrace();
        } catch (RemotingException e) {
            e.printStackTrace();
        }

    }

    private TopicRouteData getTopicRouteData(int perm1, int perm2) {
        TopicRouteData topicRouteData = new TopicRouteData();
        List<QueueData> queueDataList = Lists.newArrayList();
        QueueData d1 = new QueueData();
        d1.setBrokerName("broker-a");
        d1.setPerm(perm1);
        d1.setReadQueueNums(2);
        d1.setWriteQueueNums(2);
        d1.setTopicSynFlag(1);
        queueDataList.add(d1);

        QueueData d2 = new QueueData();
        d2.setBrokerName("broker-b");
        d2.setPerm(perm2);
        d2.setReadQueueNums(2);
        d2.setWriteQueueNums(2);
        d2.setTopicSynFlag(1);
        queueDataList.add(d2);

        topicRouteData.setQueueDatas(queueDataList);
        return topicRouteData;
    }


    private ClusterInfo getClusterInfo() {
        ClusterInfo info = new ClusterInfo();
        HashMap<String, BrokerData> brokerAddrTable = new HashMap<String, BrokerData>();
        info.setBrokerAddrTable(brokerAddrTable);
        BrokerData b1 = new BrokerData();
        b1.setBrokerName("broker-a");
        b1.setBrokerAddrs((HashMap<Long, String>) Maps.newHashMap(MixAll.MASTER_ID, "127.0.0.1:10911"));
        brokerAddrTable.put("broker-a", b1);

        BrokerData b2 = new BrokerData();
        b2.setBrokerName("broker-b");
        b2.setBrokerAddrs((HashMap<Long, String>) Maps.newHashMap(MixAll.MASTER_ID, "127.0.0.2:10911"));
        brokerAddrTable.put("broker-b", b2);

        HashMap<String, Set<String>> cluster = new HashMap<>();
        info.setClusterAddrTable(cluster);
        cluster.put("TestCluster", Sets.newHashSet("broker-a", "broker-b"));

        return info;

    }


    @Test
    public void testExecute() {
        UpdateTopicPermSubCommand cmd = new UpdateTopicPermSubCommand();
        Options options = ServerUtil.buildCommandlineOptions(new Options());
        String[] subargs = new String[]{"-b 127.0.0.1:10911", "-c default-cluster", "-t unit-test", "-p 6"};
        final CommandLine commandLine =
                ServerUtil.parseCmdLine("mqadmin " + cmd.commandName(), subargs, cmd.buildCommandlineOptions(options), new PosixParser());
        assertThat(commandLine.getOptionValue('b').trim()).isEqualTo("127.0.0.1:10911");
        assertThat(commandLine.getOptionValue('c').trim()).isEqualTo("default-cluster");
        assertThat(commandLine.getOptionValue('t').trim()).isEqualTo("unit-test");
        assertThat(commandLine.getOptionValue('p').trim()).isEqualTo("6");

    }


    @Test
    public void testIllegalPerm() {
        final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
        System.setOut(new PrintStream(outContent));
        Options options = ServerUtil.buildCommandlineOptions(new Options());
        String[] subargs = new String[]{"-b 127.0.0.1:10911", "-t test_perm_11", "-p 3"};
        final CommandLine commandLine =
                ServerUtil.parseCmdLine("mqadmin " + cmd.commandName(), subargs, cmd.buildCommandlineOptions(options), new PosixParser());
        try {
            cmd.execute(commandLine, options, null);
            assertThat(outContent.toString()).containsSequence("perm only support 2(W), 4(R) or 6(RW)");
        } catch (SubCommandException e) {
            e.printStackTrace();
        }

    }

    @Test
    public void testUpdatePermByBrokerForDifferentPerm() throws RemotingException, MQClientException, InterruptedException {
        final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
        System.setOut(new PrintStream(outContent));


        Options options = ServerUtil.buildCommandlineOptions(new Options());
        String[] subargsA = new String[]{"-b 127.0.0.1:10911", "-t test_perm_11", "-p 6"};

        final CommandLine commandLineA =
                ServerUtil.parseCmdLine("mqadmin " + cmd.commandName(), subargsA, cmd.buildCommandlineOptions(options), new PosixParser());


        TopicRouteData topicRouteData = getTopicRouteData(2, 6);
        when(mqAdminExt.examineTopicRouteInfo(anyString())).thenReturn(topicRouteData);
        try {
            cmd.execute(commandLineA, options, null);
            assertThat(outContent.toString()).containsSequence("update topic perm to 6 in 127.0.0.1:10911 success");
            assertThat(outContent.toString()).doesNotContain("update topic perm to 6 in 127.0.0.2:10911 success");
        } catch (SubCommandException e) {
            e.printStackTrace();
        }

    }


    @Test
    public void testUpdatePermByBrokerForSamePerm() throws RemotingException, MQClientException, InterruptedException {
        final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
        System.setOut(new PrintStream(outContent));


        Options options = ServerUtil.buildCommandlineOptions(new Options());
        String[] subargsA = new String[]{"-b 127.0.0.1:10911", "-t test_perm_11", "-p 6"};
        String[] subargsB = new String[]{"-b 127.0.0.2:10911", "-t test_perm_11", "-p 6"};

        final CommandLine commandLineA =
                ServerUtil.parseCmdLine("mqadmin " + cmd.commandName(), subargsA, cmd.buildCommandlineOptions(options), new PosixParser());

        final CommandLine commandLineB =
                ServerUtil.parseCmdLine("mqadmin " + cmd.commandName(), subargsB, cmd.buildCommandlineOptions(options), new PosixParser());


        TopicRouteData topicRouteData = getTopicRouteData(2, 6);
        when(mqAdminExt.examineTopicRouteInfo(anyString())).thenReturn(topicRouteData);

        try {
            cmd.execute(commandLineB, options, null);
            cmd.execute(commandLineA, options, null);
            assertThat(outContent.toString()).containsSequence("update topic perm to 6 in 127.0.0.1:10911 success");
            assertThat(outContent.toString()).containsSequence("new perm equals to the old one on 127.0.0.2:10911");
        } catch (SubCommandException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testUpdatePermByCluster() throws RemotingException, MQClientException, InterruptedException {

        final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
        System.setOut(new PrintStream(outContent));

        TopicRouteData topicRouteData = getTopicRouteData(2, 6);
        when(mqAdminExt.examineTopicRouteInfo(anyString())).thenReturn(topicRouteData);

        Options options = ServerUtil.buildCommandlineOptions(new Options());
        String[] subargs = new String[]{"-c TestCluster", "-t test_perm_11", "-p 6"};
        final CommandLine commandLine =
                ServerUtil.parseCmdLine("mqadmin " + cmd.commandName(), subargs, cmd.buildCommandlineOptions(options), new PosixParser());

        try {
            cmd.execute(commandLine, options, null);
            assertThat(outContent.toString()).containsSequence("update topic perm to 6 in 127.0.0.1:10911 success");
            assertThat(outContent.toString()).containsSequence("new perm equals to the old one on 127.0.0.2:10911");

        } catch (SubCommandException e) {
            e.printStackTrace();
        }

    }

    @Test
    public void testUpdatePerm() {
        UpdateTopicPermSubCommand cmd = new UpdateTopicPermSubCommand();


        Options options = ServerUtil.buildCommandlineOptions(new Options());
        String[] subargs = new String[]{"-b 10.9.20.107:10911", "-t test_perm_11", "-p 6"};

//        String[] subargs = new String[]{"-c BackupCluster", "-t test_perm_11", "-p 2"};
        final CommandLine commandLine =
                ServerUtil.parseCmdLine("mqadmin " + cmd.commandName(), subargs, cmd.buildCommandlineOptions(options), new PosixParser());

        try {
            System.setProperty(MixAll.NAMESRV_ADDR_PROPERTY, "10.9.20.106:9876");

            cmd.execute(commandLine, options, null);


        } catch (SubCommandException e) {
            e.printStackTrace();
        }

    }
}