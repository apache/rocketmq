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
package org.apache.rocketmq.tools.command.cluster;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.protocol.body.ClusterInfo;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.SubCommandException;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Created by zhaiyi on 2018/11/8.
 */
public class ClusterSendMsgRTCommandTest {
    private static CLusterSendMsgRTCommand clusterSendMsgRTCommand;

    @BeforeClass
    public static void init() throws NoSuchFieldException, IllegalAccessException, InterruptedException, MQBrokerException, RemotingException, MQClientException {
        clusterSendMsgRTCommand = new CLusterSendMsgRTCommand();
        DefaultMQProducer producer = mock(DefaultMQProducer.class);
        DefaultMQAdminExt adminExt = mock(DefaultMQAdminExt.class);
        Field proField = CLusterSendMsgRTCommand.class.getDeclaredField("producer");
        proField.setAccessible(true);
        proField.set(clusterSendMsgRTCommand, producer);
        Field admField = CLusterSendMsgRTCommand.class.getDeclaredField("adminExt");
        admField.setAccessible(true);
        admField.set(clusterSendMsgRTCommand, adminExt);
        SendResult sendResult = new SendResult();
        sendResult.setSendStatus(SendStatus.SEND_OK);
        ClusterInfo clusterInfo = new ClusterInfo();
        HashMap<String, Set<String>> map = new HashMap<>();
        Set<String> set = new HashSet<>();
        set.add("broker-a");
        map.put("DefaultCluster", set);
        clusterInfo.setClusterAddrTable(map);
        when(adminExt.examineBrokerClusterInfo()).thenReturn(clusterInfo);
        when(producer.send(any(Message.class))).thenReturn(sendResult);
    }

    @AfterClass
    public static void terminal() {

    }

    @Test
    public void test() throws InterruptedException, SubCommandException {
        PrintStream out = System.out;
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        System.setOut(new PrintStream(outputStream));
        Options options = ServerUtil.buildCommandlineOptions(new Options());
        String[] subargs = new String[] {"-a 1", "-s 100", "-i 1", "-t 1"};
        CommandLine commandLine = ServerUtil.parseCmdLine("mqadmin " + clusterSendMsgRTCommand.commandName(),
            subargs, clusterSendMsgRTCommand.buildCommandlineOptions(options), new PosixParser());
        clusterSendMsgRTCommand.execute(commandLine, options, null);
        String res = new String(outputStream.toByteArray());
        res = res.replaceAll("\\n", "  ");
        System.setOut(out);
        String[] ss = res.split("\\s{2,}");
        double d = Double.parseDouble(ss[7]);
        int succeeCount = Integer.parseInt(ss[8]);
        Assert.assertTrue(d != Double.NaN && d > 0);
        Assert.assertEquals(succeeCount, 1);
    }
}
