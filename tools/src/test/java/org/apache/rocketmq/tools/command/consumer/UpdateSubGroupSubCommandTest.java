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
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.common.protocol.route.QueueData;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.common.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.CommandUtil;
import org.apache.rocketmq.tools.command.SubCommandException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;

@RunWith(MockitoJUnitRunner.class)
public class UpdateSubGroupSubCommandTest {

    @Mock
    private DefaultMQAdminExt defaultMQAdminExt;

    @Mock
    private CommandUtil commandUtil;

    private String consumerGroup = "consumer1";

    private TopicConfig topicConfig;

    @Before
    public void init() throws Exception {
        TopicRouteData topicRouteData = new TopicRouteData();
        ArrayList<BrokerData> brokerDatas = new ArrayList<BrokerData>();

        BrokerData brokerData = new BrokerData();
        brokerData.setBrokerName("broker1");
        brokerData.setCluster("cluster1");
        brokerData.setBrokerAddrs(new HashMap<Long, String>());
        brokerData.getBrokerAddrs().put(MixAll.MASTER_ID, "192.168.1.21:10911");

        brokerDatas.add(brokerData);

        topicRouteData.setBrokerDatas(brokerDatas);

        ArrayList<QueueData> queueDatas = new ArrayList<>();
        QueueData queueData = new QueueData();
        queueData.setBrokerName("broker1");
        queueData.setPerm(6);
        queueData.setReadQueueNums(1);
        queueData.setWriteQueueNums(1);
        queueDatas.add(queueData);
        topicRouteData.setQueueDatas(queueDatas);

        Mockito.when(defaultMQAdminExt.examineTopicRouteInfo(Mockito.anyString())).thenReturn(topicRouteData);
        Mockito.doNothing().when(defaultMQAdminExt).start();
        Mockito.doNothing().when(defaultMQAdminExt).shutdown();
        Mockito.doNothing().when(defaultMQAdminExt).createAndUpdateSubscriptionGroupConfig(Mockito.anyString(), Mockito.any(SubscriptionGroupConfig.class));
        Mockito.doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                topicConfig = (TopicConfig) invocationOnMock.getArguments()[1];
                return null;
            }
        }).when(defaultMQAdminExt).createAndUpdateTopicConfig(Mockito.anyString(), Mockito.any(TopicConfig.class));
    }

    @Test
    public void testExec() throws SubCommandException, NoSuchFieldException, IllegalAccessException {
        UpdateSubGroupSubCommand updateSubGroupSubCommand = new UpdateSubGroupSubCommand();
        Field defaultMQAdminExtMethod = UpdateSubGroupSubCommand.class.getDeclaredField("defaultMQAdminExt");
        defaultMQAdminExtMethod.setAccessible(true);
        defaultMQAdminExtMethod.set(updateSubGroupSubCommand, defaultMQAdminExt);
        Options options = ServerUtil.buildCommandlineOptions(new Options());
        String[] subargs = new String[]{"-g " + consumerGroup, "-q 8", "-b 192.168.1.21:10911"};
        final CommandLine commandLine =
            ServerUtil.parseCmdLine("mqadmin " + updateSubGroupSubCommand.commandName(), subargs, updateSubGroupSubCommand.buildCommandlineOptions(options), new PosixParser());
        updateSubGroupSubCommand.execute(commandLine, options, null);
        Assert.assertEquals(topicConfig.getWriteQueueNums(), 8);
    }


}
