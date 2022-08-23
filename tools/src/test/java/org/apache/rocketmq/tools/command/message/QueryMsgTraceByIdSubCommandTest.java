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
package org.apache.rocketmq.tools.command.message;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.common.protocol.route.QueueData;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.apache.rocketmq.tools.command.SubCommandException;
import org.apache.rocketmq.tools.command.server.ServerResponseMocker;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class QueryMsgTraceByIdSubCommandTest {

    private ServerResponseMocker brokerMocker;

    private ServerResponseMocker nameServerMocker;

    private static final String MSG_ID = "AC1FF54E81C418B4AAC24F92E1E00000";

    @Before
    public void before() {
        brokerMocker = startOneBroker();
        nameServerMocker = startNameServer();
    }

    @After
    public void after() {
        brokerMocker.shutdown();
        nameServerMocker.shutdown();
    }

    @Test
    public void testExecute() throws SubCommandException {
        QueryMsgTraceByIdSubCommand cmd = new QueryMsgTraceByIdSubCommand();
        Options options = ServerUtil.buildCommandlineOptions(new Options());
        String[] subargs = new String[] {String.format("-i %s", MSG_ID),
            String.format("-n localhost:%d", nameServerMocker.listenPort())};
        final CommandLine commandLine =
                ServerUtil.parseCmdLine("mqadmin " + cmd.commandName(), subargs, cmd.buildCommandlineOptions(options), new PosixParser());
        cmd.execute(commandLine, options, null);
    }

    private ServerResponseMocker startNameServer() {
        TopicRouteData topicRouteData = new TopicRouteData();
        List<BrokerData> dataList = new ArrayList<>();
        HashMap<Long, String> brokerAddress = new HashMap<>();
        brokerAddress.put(1L, "127.0.0.1:" + brokerMocker.listenPort());
        BrokerData brokerData = new BrokerData("mockCluster", "mockBrokerName", brokerAddress);
        brokerData.setBrokerName("mockBrokerName");
        dataList.add(brokerData);
        topicRouteData.setBrokerDatas(dataList);

        List<QueueData> queueDatas = new ArrayList<>();
        QueueData queueData = new QueueData();
        queueData.setBrokerName("mockBrokerName");
        queueData.setPerm(1);
        queueData.setReadQueueNums(1);
        queueData.setTopicSysFlag(1);
        queueData.setWriteQueueNums(1);
        queueDatas.add(queueData);
        topicRouteData.setQueueDatas(queueDatas);

        return ServerResponseMocker.startServer(0, topicRouteData.encode());
    }

    private ServerResponseMocker startOneBroker() {
        try {
            MessageExt messageExt = new MessageExt();
            messageExt.setTopic(TopicValidator.RMQ_SYS_TRACE_TOPIC);
            messageExt.setBody(new byte[100]);
            // topic RMQ_SYS_TRACE_TOPIC which built-in rocketMQ, set msg id as msg key
            messageExt.setKeys(MSG_ID);
            messageExt.setBornHost(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 0));
            messageExt.setStoreHost(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 0));
            byte[] body = MessageDecoder.encode(messageExt, false);

            HashMap<String, String> extMap = new HashMap<>();
            extMap.put("indexLastUpdateTimestamp", String.valueOf(System.currentTimeMillis()));
            extMap.put("indexLastUpdatePhyoffset", String.valueOf(System.currentTimeMillis()));
            // start broker
            return ServerResponseMocker.startServer(0, body, extMap);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }
}