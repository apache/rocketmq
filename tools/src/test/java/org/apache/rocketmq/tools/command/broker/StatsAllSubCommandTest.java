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

import com.google.common.collect.Sets;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.MQClientAPIImpl;
import org.apache.rocketmq.client.impl.MQClientManager;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.common.admin.ConsumeStats;
import org.apache.rocketmq.common.admin.OffsetWrapper;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.body.BrokerStatsData;
import org.apache.rocketmq.common.protocol.body.BrokerStatsItem;
import org.apache.rocketmq.common.protocol.body.GroupList;
import org.apache.rocketmq.common.protocol.body.TopicList;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExtImpl;
import org.apache.rocketmq.tools.command.SubCommandException;
import org.apache.rocketmq.tools.command.stats.StatsAllSubCommand;
import org.assertj.core.util.Lists;
import org.assertj.core.util.Maps;
import org.junit.BeforeClass;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.HashSet;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class StatsAllSubCommandTest {

    private static StatsAllSubCommand cmd = new StatsAllSubCommand();

    private static DefaultMQAdminExt defaultMQAdminExt;
    private static DefaultMQAdminExtImpl defaultMQAdminExtImpl;
    private static MQClientInstance mqClientInstance = MQClientManager.getInstance().getAndCreateMQClientInstance(new ClientConfig());
    private static MQClientAPIImpl mQClientAPIImpl;

    @BeforeClass
    public static void init() throws NoSuchFieldException, IllegalAccessException, InterruptedException, RemotingException, MQClientException, MQBrokerException {
        mQClientAPIImpl = mock(MQClientAPIImpl.class);
        defaultMQAdminExt = mock(DefaultMQAdminExt.class);
        defaultMQAdminExtImpl = new DefaultMQAdminExtImpl(defaultMQAdminExt, 1000);

        Field field = DefaultMQAdminExtImpl.class.getDeclaredField("mqClientInstance");
        field.setAccessible(true);
        field.set(defaultMQAdminExtImpl, mqClientInstance);
        field = MQClientInstance.class.getDeclaredField("mQClientAPIImpl");
        field.setAccessible(true);
        field.set(mqClientInstance, mQClientAPIImpl);
        field = DefaultMQAdminExt.class.getDeclaredField("defaultMQAdminExtImpl");
        field.setAccessible(true);
        field.set(defaultMQAdminExt, defaultMQAdminExtImpl);

        TopicList topicList = new TopicList();
        topicList.setBrokerAddr("127.0.0.1:10911");
        HashSet<String> topics = Sets.newHashSet("active");
        topicList.setTopicList(topics);
        when(defaultMQAdminExt.fetchAllTopicList()).thenReturn(topicList);
        TopicRouteData topicRouteData = new TopicRouteData();
        BrokerData brokerData = new BrokerData();
        brokerData.setBrokerAddrs((HashMap<Long, String>) Maps.newHashMap(0L, "127.0.0.1:10911"));
        topicRouteData.setBrokerDatas(Lists.newArrayList(brokerData));
        GroupList groupList = new GroupList();
        groupList.setGroupList(Sets.<String>newHashSet("default-cluster"));
        when(defaultMQAdminExt.queryTopicConsumeByWho(anyString())).thenReturn(groupList);
        when(defaultMQAdminExt.examineTopicRouteInfo(anyString())).thenReturn(topicRouteData);
        when(mQClientAPIImpl.queryTopicConsumeByWho(anyString(), anyString(), anyLong())).thenReturn(groupList);
        ConsumeStats consumeStats = new ConsumeStats();
        consumeStats.setConsumeTps(1000000);
        consumeStats.setOffsetTable(new HashMap<MessageQueue, OffsetWrapper>());
        when(defaultMQAdminExt.examineConsumeStats(anyString(), anyString())).thenReturn(consumeStats);
        BrokerStatsData brokerStatsData = new BrokerStatsData();
        BrokerStatsItem brokerStatsDay = new BrokerStatsItem();
        brokerStatsDay.setSum(24 * 60 * 1000000);
        brokerStatsDay.setAvgpt(24 * 60 * 1000000);
        brokerStatsDay.setTps(24 * 60 * 1000000);
        BrokerStatsItem brokerStatsHour = new BrokerStatsItem();
        brokerStatsHour.setSum(60 * 1000000);
        brokerStatsHour.setAvgpt(60 * 1000000);
        brokerStatsHour.setTps(60 * 1000000);
        BrokerStatsItem brokerStatsMinute = new BrokerStatsItem();
        brokerStatsMinute.setSum(1000000);
        brokerStatsMinute.setAvgpt(1000000);
        brokerStatsMinute.setTps(1000000);
        brokerStatsData.setStatsDay(brokerStatsDay);
        brokerStatsData.setStatsHour(brokerStatsHour);
        brokerStatsData.setStatsMinute(brokerStatsMinute);
        when(defaultMQAdminExt.viewBrokerStatsData(anyString(), anyString(), anyString())).thenReturn(brokerStatsData);
    }

    @Test
    public void testExecute() throws SubCommandException, IllegalAccessException, NoSuchFieldException {
        Field field = StatsAllSubCommand.class.getDeclaredField("defaultMQAdminExt");
        field.setAccessible(true);
        field.set(cmd, defaultMQAdminExt);
        Options options = ServerUtil.buildCommandlineOptions(new Options());
        String[] subargs = new String[]{"-a", "-tactive"};
        final CommandLine commandLine =
                ServerUtil.parseCmdLine("mqadmin " + cmd.commandName(), subargs, cmd.buildCommandlineOptions(options), new PosixParser());
        cmd.execute(commandLine, options, null);
    }


}
