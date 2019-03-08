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

import com.google.common.collect.Lists;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.PullStatus;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.apache.rocketmq.tools.command.SubCommandException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.lang.reflect.Field;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.*;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class PrintMessageSubCommandTest {

    private static PrintMessageSubCommand printMessageSubCommand;
    private final static Random random = new Random(System.currentTimeMillis());
    @Before
    public void init() throws InterruptedException, RemotingException, MQClientException, MQBrokerException, NoSuchFieldException, IllegalAccessException {
        printMessageSubCommand = new PrintMessageSubCommand();
        DefaultMQPullConsumer defaultMQPullConsumer = mock(DefaultMQPullConsumer.class);
        MessageExt msg = new MessageExt();
        msg.setTags("tagA");
        msg.setBody("This is Body!".getBytes());
        List<MessageExt> msgFoundList = new ArrayList<>();
        msgFoundList.add(msg);
        final PullResult pullResult = new PullResult(PullStatus.FOUND, 2, 0, 1, msgFoundList);

        when(defaultMQPullConsumer.pull(any(MessageQueue.class), anyString(), anyLong(), anyInt())).thenReturn(pullResult);
        when(defaultMQPullConsumer.minOffset(any(MessageQueue.class))).thenReturn(Long.valueOf(0));
        when(defaultMQPullConsumer.maxOffset(any(MessageQueue.class))).thenReturn(Long.valueOf(1));

        final Set<MessageQueue> mqList = new HashSet<>();
        mqList.add(new MessageQueue());
        when(defaultMQPullConsumer.fetchSubscribeMessageQueues(anyString())).thenReturn(mqList);

        Field producerField = PrintMessageSubCommand.class.getDeclaredField("consumer");
        producerField.setAccessible(true);
        producerField.set(printMessageSubCommand, defaultMQPullConsumer);
    }

    @Test
    public void testTimestampFormat(){
        String longFormatTime = "1551347292123";
        long longTimeStramp = PrintMessageSubCommand.timestampFormat(longFormatTime);
        Assert.assertEquals(longTimeStramp,1551347292123L);

        String dateFormatTime = "2019-02-28#17:48:12:123";
        longTimeStramp = PrintMessageSubCommand.timestampFormat(dateFormatTime);
        Assert.assertEquals(longTimeStramp,1551347292123L);
    }

    @Test
    public void testPrintMessage() throws Exception{
        List<MessageExt> msgs = Lists.newArrayList();
        for (int i = 0; i < 3; i++){
            int queueIdInt = Math.abs(random.nextInt() % 99999999) % 2;
            long storeTimestamp = System.currentTimeMillis();
            long bornTimestamp = PrintMessageSubCommand.timestampFormat("2019-02-28#17:48:12:123");
            SocketAddress bornHost = new InetSocketAddress(InetAddress.getLocalHost(),8080);
            SocketAddress storeHost = new InetSocketAddress(InetAddress.getLocalHost(),8070);
            String msgId = "AC1067F400002A9F00000002069208A" + i;
            MessageExt msg = new MessageExt(queueIdInt,bornTimestamp,bornHost,storeTimestamp,storeHost,msgId);
            msg.setBody("This is Body!".getBytes("UTF-8"));
            msgs.add(msg);
        }

        PrintStream out = System.out;
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        System.setOut(new PrintStream(bos));

        PrintMessageSubCommand.printMessage(msgs,"UTF-8",true);

        System.setOut(out);
        String s = new String(bos.toByteArray());
        Assert.assertTrue(s.contains("This is Body"));
    }

    @Test
    public void testCommandName(){
        String name = printMessageSubCommand.commandName();
        Assert.assertEquals("printMsg", name);
    }

    @Test
    public void testCommandDesc(){
        String descr = printMessageSubCommand.commandDesc();
        Assert.assertEquals("Print Message Detail", descr);
    }

    /**
     * Option OptionValue Detail
     * -t topic topic name
     * -c charsetName CharsetName(eg: UTF-8、GBK)
     * -s subExpression Subscribe Expression(eg: TagA || TagB)
     * -b beginTimestamp Begin timestamp[currentTimeMillis|yyyy-MM-dd#HH:mm:ss:SSS]
     * -e endTimestamp End timestamp[currentTimeMillis|yyyy-MM-dd#HH:mm:ss:SSS]
     * -d printBody print body
     */
    @Test
    public void testBuildCommandlineOptions(){
        Options options = new Options();
        printMessageSubCommand.buildCommandlineOptions(options);
        Assert.assertEquals(options.getRequiredOptions().size(),1);
        Assert.assertEquals(options.getOptions().size(),6);
        Assert.assertNotNull(options.getOption("t"));
        Assert.assertNotNull(options.getOption("c"));
        Assert.assertNotNull(options.getOption("s"));
        Assert.assertNotNull(options.getOption("b"));
        Assert.assertNotNull(options.getOption("e"));
        Assert.assertNotNull(options.getOption("d"));
    }

    @Test
    public void testExecute() throws SubCommandException {
        PrintStream out = System.out;
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        System.setOut(new PrintStream(bos));
        String[] subargs = new String[] {"-t TopicTest","-c UTF-8","-s TagA",
                "-b 2019-03-01#19:07:01:123","-d true"};
        Options options = new Options();
        CommandLine commandLine = ServerUtil.parseCmdLine("mqadmin " + printMessageSubCommand.commandName(),
                subargs, printMessageSubCommand.buildCommandlineOptions(options), new PosixParser());

        printMessageSubCommand.execute(commandLine,options,null);
        System.setOut(out);
        String s = new String(bos.toByteArray());
        Assert.assertTrue(s.contains("This is Body"));
    }
}

