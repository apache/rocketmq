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
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.io.FileUtils;
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
import org.assertj.core.util.Lists;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ExportMessageCommandTest {
    private static ExportMessageCommand exportMessageCommand;

    private static final PullResult PULL_RESULT = mockPullResult();
    private static String exportDir = "./rocketmq-export";
    private static String topicDir = exportDir + File.separator + "topic1";
    private static String brokerDir = topicDir + File.separator + "broker-a";
    private static String queueFilePath = brokerDir + File.separator + "1";
    private static String offsetFilePath = queueFilePath + ".log";

    private static PullResult mockPullResult() {
        MessageExt msg = new MessageExt();
        MessageExt messageExt = new MessageExt();
        messageExt.setTopic("topic1");
        messageExt.setBrokerName("broker-a");
        messageExt.setQueueId(1);
        msg.setBody("{'age':1}".getBytes(StandardCharsets.UTF_8));
        messageExt.setBornHost(new InetSocketAddress(10910));
        messageExt.setStoreHost(new InetSocketAddress(10910));
        messageExt.setDeliverTimeMs(1L);
        List<MessageExt> msgFoundList = new ArrayList<>();
        msgFoundList.add(msg);
        return new PullResult(PullStatus.FOUND, 1, 0, 1, msgFoundList);
    }

    @BeforeClass
    public static void init() throws MQClientException, RemotingException, MQBrokerException, InterruptedException,
        NoSuchFieldException, IllegalAccessException {
        exportMessageCommand = new ExportMessageCommand();
        DefaultMQPullConsumer defaultMQPullConsumer = mock(DefaultMQPullConsumer.class);

        assignPullResult(defaultMQPullConsumer);
        when(defaultMQPullConsumer.minOffset(any(MessageQueue.class))).thenReturn(Long.valueOf(0));
        when(defaultMQPullConsumer.maxOffset(any(MessageQueue.class))).thenReturn(Long.valueOf(1));

        final Set<MessageQueue> mqList = new HashSet<>();
        mqList.add(new MessageQueue("topic1", "broker-a", 1));
        when(defaultMQPullConsumer.fetchSubscribeMessageQueues(anyString())).thenReturn(mqList);

        Field producerField = ExportMessageCommand.class.getDeclaredField("defaultMQPullConsumer");
        producerField.setAccessible(true);
        producerField.set(exportMessageCommand, defaultMQPullConsumer);
    }

    @AfterClass
    public static void terminate() throws IOException {

    }

    @Before
    public void before() throws IOException {
        new File(exportDir).deleteOnExit();
    }

    @After
    public void after() throws IOException {
        new File(exportDir).deleteOnExit();
    }

    private static void assignPullResult() {
        assignPullResult(null);
    }

    private static void assignPullResult(DefaultMQPullConsumer defaultMQPullConsumer) {
        try {
            if (defaultMQPullConsumer == null) {
                Field producerField = ExportMessageCommand.class.getDeclaredField("defaultMQPullConsumer");
                producerField.setAccessible(true);
                defaultMQPullConsumer = (DefaultMQPullConsumer) producerField.get(exportMessageCommand);
            }
            when(defaultMQPullConsumer.pull(any(MessageQueue.class), anyString(), anyLong(), anyInt()))
                .thenReturn(PULL_RESULT);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testExecuteDefault() throws Exception {
        exportDir = "./rocketmq-export-" + UUID.randomUUID();
        String result = basicExport("base64");
        Assert.assertTrue(result.contains("100%"));
    }

    @Test
    public void testExecuteJSON() throws Exception {
        exportDir = "./rocketmq-export-" + UUID.randomUUID();
        String result = basicExport("json");
        Assert.assertTrue(result.contains("100%"));
    }

    private String basicExport(String format) throws SubCommandException {
        PrintStream out = System.out;
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        System.setOut(new PrintStream(bos));
        Options options = ServerUtil.buildCommandlineOptions(new Options());
        String[] subargs = new String[] {"-t topic1", "-n localhost:9876", "-f " + format, "-d " + exportDir};
        assignPullResult();
        CommandLine commandLine = ServerUtil.parseCmdLine("mqadmin " + exportMessageCommand.commandName(),
            subargs, exportMessageCommand.buildCommandlineOptions(options), new DefaultParser());
        exportMessageCommand.execute(commandLine, options, null);

        System.setOut(out);
        return new String(bos.toByteArray(), StandardCharsets.UTF_8);
    }

    @Test
    public void testRecoverFromOffsetFileWithLessBeginOffset() throws Exception {
        exportDir = ExportMessageCommand.DEFAULT_EXPORT_DIRECTORY;
        prepareMessageFileAndOffsetFile(-1, 1);
        Assert.assertTrue(basicExport("base64").contains("100%"));

    }

    @Test
    public void testRecoverFromOffsetFileWithGraterEndOffset() throws Exception {
        exportDir = ExportMessageCommand.DEFAULT_EXPORT_DIRECTORY;
        prepareMessageFileAndOffsetFile(0, 2);
        Assert.assertTrue(basicExport("base64").contains("100%"));
    }

    @Test
    public void testRecoverFromOffsetFileNormal() throws Exception {
        exportDir = ExportMessageCommand.DEFAULT_EXPORT_DIRECTORY;
        prepareMessageFileAndOffsetFile(0, 1);
        // skip export (minOffset and maxOffset) is the same as offsetFile
        String result = basicExport("base64");
        Assert.assertFalse(result.contains("100%"));
    }

    public void prepareMessageFileAndOffsetFile(long beginOffset, long endOffset) throws Exception {
        FileUtils.forceMkdirParent(new File(brokerDir));
        String exportedMessageText = "{\"topic\":\"topic1\",\"flag\":0,\"queueOffset\":0,\"bodyFormat\":\"json\",\"body\":{\"age\":1}}";
        FileUtils.writeLines(new File(queueFilePath), Lists.newArrayList(exportedMessageText));
        FileUtils.write(new File(offsetFilePath), String.format("{\"beginOffset\":%d,\"endOffset\":%d}", beginOffset, endOffset), StandardCharsets.UTF_8);
    }

}