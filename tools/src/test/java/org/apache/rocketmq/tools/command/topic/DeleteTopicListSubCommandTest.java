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

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.MissingOptionException;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.body.DeleteTopicListRequestBody;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.apache.rocketmq.tools.command.SubCommandException;
import org.apache.rocketmq.tools.command.server.NameServerMocker;
import org.apache.rocketmq.tools.command.server.ServerResponseMocker;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;

public class DeleteTopicListSubCommandTest {

    private ServerResponseMocker brokerMocker;

    private ServerResponseMocker nameServerMocker;

    private File topicFile;

    @Before
    public void before() throws Exception {
        brokerMocker = ServerResponseMocker.startServer(null);
        nameServerMocker = NameServerMocker.startByDefaultConf(brokerMocker.listenPort());
        System.setProperty(MixAll.NAMESRV_ADDR_PROPERTY, "localhost:" + nameServerMocker.listenPort());

        topicFile = File.createTempFile("mqadmin-topic-list", ".json");
        topicFile.deleteOnExit();
        Files.write(topicFile.toPath(), "[\"topicA\",\"topicB\"]".getBytes(StandardCharsets.UTF_8));
    }

    @After
    public void after() {
        System.clearProperty(MixAll.NAMESRV_ADDR_PROPERTY);
        brokerMocker.shutdown();
        nameServerMocker.shutdown();
    }

    @Test
    public void testArguments() {
        DeleteTopicListSubCommand cmd = new DeleteTopicListSubCommand();
        Options options = ServerUtil.buildCommandlineOptions(new Options());
        String[] subargs = new String[] {"-b 127.0.0.1:10911", "-f topics.json"};
        final CommandLine commandLine =
            ServerUtil.parseCmdLine("mqadmin " + cmd.commandName(), subargs,
                cmd.buildCommandlineOptions(options), new DefaultParser());

        assertEquals("127.0.0.1:10911", commandLine.getOptionValue('b').trim());
        assertEquals("topics.json", commandLine.getOptionValue('f').trim());
    }

    @Test
    public void testArgumentsWithCluster() {
        DeleteTopicListSubCommand cmd = new DeleteTopicListSubCommand();
        Options options = ServerUtil.buildCommandlineOptions(new Options());
        String[] subargs = new String[] {"-c DefaultCluster", "-f topics.json"};
        final CommandLine commandLine =
            ServerUtil.parseCmdLine("mqadmin " + cmd.commandName(), subargs,
                cmd.buildCommandlineOptions(options), new DefaultParser());

        assertEquals("DefaultCluster", commandLine.getOptionValue('c').trim());
        assertFalse(commandLine.hasOption('b'));
    }

    @Test
    public void testBrokerOrClusterIsRequired() {
        DeleteTopicListSubCommand cmd = new DeleteTopicListSubCommand();
        Options options = cmd.buildCommandlineOptions(ServerUtil.buildCommandlineOptions(new Options()));

        MissingOptionException exception = assertThrows(MissingOptionException.class,
            () -> new DefaultParser().parse(options, new String[] {"-f topics.json"}));
        assertNotNull(exception.getMessage());
    }

    @Test
    public void testExecuteDeleteTopicListInBroker() throws SubCommandException {
        DeleteTopicListSubCommand cmd = new DeleteTopicListSubCommand();
        Options options = ServerUtil.buildCommandlineOptions(new Options());
        String[] subargs = new String[] {
            "-b 127.0.0.1:" + brokerMocker.listenPort(),
            "-f " + topicFile.getAbsolutePath()};
        final CommandLine commandLine =
            ServerUtil.parseCmdLine("mqadmin " + cmd.commandName(), subargs,
                cmd.buildCommandlineOptions(options), new DefaultParser());

        cmd.execute(commandLine, options, null);

        RemotingCommand request = brokerMocker.getLastRequestCommand();
        assertNotNull(request);
        assertEquals(RequestCode.DELETE_TOPIC_IN_BROKER_LIST, request.getCode());

        DeleteTopicListRequestBody requestBody =
            DeleteTopicListRequestBody.decode(request.getBody(), DeleteTopicListRequestBody.class);
        assertEquals(Arrays.asList("topicA", "topicB"), requestBody.getTopicList());
    }

    @Test
    public void testMissingFileDoesNotTouchBroker() throws SubCommandException {
        DeleteTopicListSubCommand cmd = new DeleteTopicListSubCommand();
        Options options = ServerUtil.buildCommandlineOptions(new Options());
        String[] subargs = new String[] {
            "-b 127.0.0.1:" + brokerMocker.listenPort(),
            "-f " + new File("mqadmin-missing-topic-list.json").getAbsolutePath()};
        final CommandLine commandLine =
            ServerUtil.parseCmdLine("mqadmin " + cmd.commandName(), subargs,
                cmd.buildCommandlineOptions(options), new DefaultParser());

        cmd.execute(commandLine, options, null);

        // an unreadable input file must short circuit before any request reaches the broker
        assertNull(brokerMocker.getLastRequestCommand());
    }
}
