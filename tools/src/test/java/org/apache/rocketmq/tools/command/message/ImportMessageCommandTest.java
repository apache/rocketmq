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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.lang.reflect.Field;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.io.FileUtils;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.apache.rocketmq.tools.command.SubCommandException;
import org.assertj.core.util.Lists;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ImportMessageCommandTest {

    private static ImportMessageCommand importMessageCommand = new ImportMessageCommand();

    @BeforeClass
    public static void init() throws MQClientException, RemotingException, InterruptedException, MQBrokerException, NoSuchFieldException, IllegalAccessException {
        DefaultMQProducer defaultMQProducer = mock(DefaultMQProducer.class);
        SendResult sendResult = new SendResult();
        sendResult.setMessageQueue(new MessageQueue());
        sendResult.getMessageQueue().setBrokerName("broker-1");
        sendResult.getMessageQueue().setQueueId(1);
        sendResult.setSendStatus(SendStatus.SEND_OK);
        sendResult.setMsgId("fgwejigherughwueyutyu4t4343t43");

        when(defaultMQProducer.send(anyList())).thenReturn(sendResult);
        when(defaultMQProducer.send(anyList(), any(MessageQueue.class))).thenReturn(sendResult);

        Field producerField = ImportMessageCommand.class.getDeclaredField("producer");
        producerField.setAccessible(true);
        producerField.set(importMessageCommand, defaultMQProducer);
    }

    @AfterClass
    public static void terminate() {
    }

    @Test
    public void testExecuteDefault() throws SubCommandException, IOException {
        String importDir = "./rocketmq-import";
        String topicDir = importDir + File.separator + "topic1";
        String brokerDir = topicDir + File.separator + "broker-a";
        FileUtils.forceMkdirParent(new File(brokerDir));
        File queueFile = new File(brokerDir + File.separator + "1");
        String exportedMessageText = "{\"topic\":\"topic1\",\"flag\":0,\"queueOffset\":0,\"bodyFormat\":\"json\",\"body\":{\"age\":1}}";
        FileUtils.writeLines(queueFile, Lists.newArrayList(exportedMessageText));

        PrintStream out = System.out;
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        System.setOut(new PrintStream(bos));
        Options options = ServerUtil.buildCommandlineOptions(new Options());
        String[] subargs = new String[] {"-d " + importDir, "-n localhost:9876"};
        CommandLine commandLine = ServerUtil.parseCmdLine("mqadmin " + importMessageCommand.commandName(),
            subargs, importMessageCommand.buildCommandlineOptions(options), new DefaultParser());
        importMessageCommand.execute(commandLine, options, null);

        System.setOut(out);
        String s = new String(bos.toByteArray());
        Assert.assertTrue(s.contains("100%"));
        FileUtils.forceDeleteOnExit(new File(importDir));
    }
}