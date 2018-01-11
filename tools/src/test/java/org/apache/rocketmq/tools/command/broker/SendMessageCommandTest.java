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
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.apache.rocketmq.tools.command.SubCommandException;
import org.apache.rocketmq.tools.command.message.SendMessageCommand;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.lang.reflect.Field;

import static org.mockito.Mockito.doAnswer;

@RunWith(org.mockito.junit.MockitoJUnitRunner.class)
public class SendMessageCommandTest {

    private SendMessageCommand sendMessageCommand = new SendMessageCommand();
    @Mock
    private DefaultMQProducer defaultMQProducer;

    private SendResult sendResult;

    private SendResult sendResultByQueue;

    @Before
    public void init() throws MQClientException, SubCommandException, RemotingException, InterruptedException, MQBrokerException, NoSuchFieldException, IllegalAccessException {
        sendResult = new SendResult();
        sendResult.setMessageQueue(new MessageQueue());
        sendResult.getMessageQueue().setBrokerName("broker1");
        sendResult.getMessageQueue().setQueueId(1);
        sendResult.setSendStatus(SendStatus.SEND_OK);
        sendResult.setMsgId("fgwejigherughwueyutyu4t4343t43");

        sendResultByQueue = sendResult;


        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                return sendResult;
            }
        }).when(defaultMQProducer).send(Mockito.any(Message.class));

        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                return sendResultByQueue;
            }
        }).when(defaultMQProducer).send(Mockito.any(Message.class),Mockito.any(MessageQueue.class));

        Field producerField = SendMessageCommand.class.getDeclaredField("producer");
        producerField.setAccessible(true);
        producerField.set(sendMessageCommand,defaultMQProducer);
    }

    @After
    public void terminate() {

    }

    @Test
    public void testExecute() throws SubCommandException {
        PrintStream out = System.out;
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        System.setOut(new PrintStream(bos));
        Options options = ServerUtil.buildCommandlineOptions(new Options());
        String[] subargs = new String[] {"-t mytopic","-b 'send message test'","-g tagA","-k order-16546745756"};
        CommandLine commandLine =
            ServerUtil.parseCmdLine("mqadmin " + sendMessageCommand.commandName(), subargs, sendMessageCommand.buildCommandlineOptions(options), new PosixParser());
        sendMessageCommand.execute(commandLine, options, null);

        subargs = new String[] {"-t mytopic","-b 'send message test'","-g tagA","-k order-16546745756","-q brokera","-i 1"};
        commandLine = ServerUtil.parseCmdLine("mqadmin " + sendMessageCommand.commandName(), subargs, sendMessageCommand.buildCommandlineOptions(options), new PosixParser());
        sendMessageCommand.execute(commandLine, options, null);
        System.setOut(out);
        String s = new String(bos.toByteArray());
        Assert.assertTrue(s.contains("SEND_OK"));
    }


}
