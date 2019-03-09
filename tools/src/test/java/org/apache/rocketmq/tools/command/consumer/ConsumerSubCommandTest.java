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
package org.apache.rocketmq.tools.command.consumer;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.MQClientAPIImpl;
import org.apache.rocketmq.client.impl.MQClientManager;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.body.*;
import org.apache.rocketmq.common.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.common.protocol.route.QueueData;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.remoting.exception.*;
import org.apache.rocketmq.remoting.protocol.LanguageCode;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExtImpl;
import org.apache.rocketmq.tools.command.SubCommandException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import java.lang.reflect.Field;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@RunWith(MockitoJUnitRunner.class)
public class ConsumerSubCommandTest {
    private static DefaultMQAdminExt defaultMQAdminExt;
    private static DefaultMQAdminExtImpl defaultMQAdminExtImpl;
    private static MQClientInstance mqClientInstance = MQClientManager.getInstance().getAndCreateMQClientInstance(new ClientConfig());
    private static MQClientAPIImpl mQClientAPIImpl;
    private static ConsumerSubCommand consumerSubCommand;

    @BeforeClass
    public static void init() throws InterruptedException, RemotingException, MQClientException, MQBrokerException, NoSuchFieldException, IllegalAccessException {
        mQClientAPIImpl = mock(MQClientAPIImpl.class);
        defaultMQAdminExt = new DefaultMQAdminExt();
        defaultMQAdminExtImpl = new DefaultMQAdminExtImpl(defaultMQAdminExt, 1000);

        consumerSubCommand = new ConsumerSubCommand();
        Field field = ConsumerSubCommand.class.getDeclaredField("defaultMQAdminExt");
        field.setAccessible(true);
        field.set(consumerSubCommand, defaultMQAdminExt);
        field = DefaultMQAdminExtImpl.class.getDeclaredField("mqClientInstance");
        field.setAccessible(true);
        field.set(defaultMQAdminExtImpl, mqClientInstance);
        field = MQClientInstance.class.getDeclaredField("mQClientAPIImpl");
        field.setAccessible(true);
        field.set(mqClientInstance, mQClientAPIImpl);
        field = DefaultMQAdminExt.class.getDeclaredField("defaultMQAdminExtImpl");
        field.setAccessible(true);
        field.set(defaultMQAdminExt, defaultMQAdminExtImpl);

        //mock for the retry topic route data
        List<BrokerData> brokerDatas = new ArrayList<>();
        HashMap<Long, String> brokerAddrs = new HashMap<>();
        brokerAddrs.put(0L, "127.0.0.1:10911");
        BrokerData brokerData = new BrokerData();
        brokerData.setCluster("default-cluster");
        brokerData.setBrokerName("default-broker");
        brokerData.setBrokerAddrs(brokerAddrs);
        brokerDatas.add(brokerData);
        TopicRouteData topicRouteData = new TopicRouteData();
        topicRouteData.setBrokerDatas(brokerDatas);
        topicRouteData.setQueueDatas(new ArrayList<QueueData>());
        topicRouteData.setFilterServerTable(new HashMap<String, List<String>>());
        when(mQClientAPIImpl.getTopicRouteInfoFromNameServer(startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX),
                anyLong())).thenReturn(topicRouteData);

        ConsumerConnection consumerConnection = new ConsumerConnection();
        consumerConnection.setConsumeType(ConsumeType.CONSUME_PASSIVELY);
        consumerConnection.setMessageModel(MessageModel.CLUSTERING);
        HashSet<Connection> connections = new HashSet<>();
        Connection connection = new Connection();
        connection.setClientId("client_id_1");
        connection.setClientAddr("127.0.0.1:109111");
        connection.setLanguage(LanguageCode.JAVA);
        connection.setVersion(MQVersion.Version.V4_0_0_SNAPSHOT.ordinal());
        Connection connection2 = new Connection();
        connection2.setClientId("client_id_2");
        connection2.setClientAddr("127.0.0.1:109112");
        connection2.setLanguage(LanguageCode.JAVA);
        connection2.setVersion(MQVersion.Version.V4_0_0_SNAPSHOT.ordinal());
        connections.add(connection);
        connections.add(connection2);
        consumerConnection.setConnectionSet(connections);
        consumerConnection.setSubscriptionTable(new ConcurrentHashMap<String, SubscriptionData>());
        consumerConnection.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        when(mQClientAPIImpl.getConsumerConnectionList(anyString(), anyString(), anyLong())).thenReturn(consumerConnection);
    }

    @AfterClass
    public static void terminate() {
        defaultMQAdminExt.shutdown();
    }

    @Test
    public void testExecuteWithDifSub() throws SubCommandException, RemotingException, MQClientException, InterruptedException {
        ConsumerRunningInfo consumerRunningInfo = new ConsumerRunningInfo();
        consumerRunningInfo.getProperties().setProperty(ConsumerRunningInfo.PROP_CONSUME_TYPE, ConsumeType.CONSUME_PASSIVELY.name());
        consumerRunningInfo.getProperties().setProperty(ConsumerRunningInfo.PROP_CONSUMER_START_TIMESTAMP, System.currentTimeMillis() - 600000 + "");
        consumerRunningInfo.setMqTable(new TreeMap<MessageQueue, ProcessQueueInfo>());
        consumerRunningInfo.setStatusTable(new TreeMap<String, ConsumeStatus>());
        TreeSet<SubscriptionData> subData = new TreeSet<SubscriptionData>();
        subData.add(new SubscriptionData("testTopic", "*"));
        consumerRunningInfo.setSubscriptionSet(subData);
        when(mQClientAPIImpl.getConsumerRunningInfo(anyString(), anyString(), eq("client_id_1"), anyBoolean(), anyLong())).thenReturn(consumerRunningInfo);

        ConsumerRunningInfo consumerRunningInfo2 = new ConsumerRunningInfo();
        consumerRunningInfo2.getProperties().setProperty(ConsumerRunningInfo.PROP_CONSUME_TYPE, ConsumeType.CONSUME_PASSIVELY.name());
        consumerRunningInfo2.getProperties().setProperty(ConsumerRunningInfo.PROP_CONSUMER_START_TIMESTAMP, System.currentTimeMillis() - 600000 + "");
        consumerRunningInfo2.setMqTable(new TreeMap<MessageQueue, ProcessQueueInfo>());
        consumerRunningInfo2.setStatusTable(new TreeMap<String, ConsumeStatus>());
        TreeSet<SubscriptionData> subData2 = new TreeSet<SubscriptionData>();
        subData2.add(new SubscriptionData("testTopic2", "*"));
        consumerRunningInfo2.setSubscriptionSet(subData2);
        when(mQClientAPIImpl.getConsumerRunningInfo(anyString(), anyString(), eq("client_id_2"), anyBoolean(), anyLong())).thenReturn(consumerRunningInfo2);

        Options options = ServerUtil.buildCommandlineOptions(new Options());
        String[] subargs = new String[]{"-g unittest"};
        CommandLine commandLine = ServerUtil.parseCmdLine("mqadmin " + consumerSubCommand.commandName(),
                subargs, consumerSubCommand.buildCommandlineOptions(options), new PosixParser());
        consumerSubCommand.execute(commandLine, options, null);
    }

    @Test
    public void testExecuteWithSameSub() throws SubCommandException, RemotingException, MQClientException, InterruptedException {
        ConsumerRunningInfo consumerRunningInfo = new ConsumerRunningInfo();
        consumerRunningInfo.getProperties().setProperty(ConsumerRunningInfo.PROP_CONSUME_TYPE, ConsumeType.CONSUME_PASSIVELY.name());
        consumerRunningInfo.getProperties().setProperty(ConsumerRunningInfo.PROP_CONSUMER_START_TIMESTAMP, System.currentTimeMillis() - 600000 + "");
        consumerRunningInfo.setMqTable(new TreeMap<MessageQueue, ProcessQueueInfo>());
        consumerRunningInfo.setStatusTable(new TreeMap<String, ConsumeStatus>());
        TreeSet<SubscriptionData> subData = new TreeSet<SubscriptionData>();
        subData.add(new SubscriptionData("testTopic", "*"));
        consumerRunningInfo.setSubscriptionSet(subData);
        when(mQClientAPIImpl.getConsumerRunningInfo(anyString(), anyString(), anyString(), anyBoolean(), anyLong())).thenReturn(consumerRunningInfo);

        Options options = ServerUtil.buildCommandlineOptions(new Options());
        String[] subargs = new String[]{"-g unittest"};
        CommandLine commandLine = ServerUtil.parseCmdLine("mqadmin " + consumerSubCommand.commandName(),
                subargs, consumerSubCommand.buildCommandlineOptions(options), new PosixParser());
        consumerSubCommand.execute(commandLine, options, null);
    }
}
