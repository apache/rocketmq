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
package org.apache.rocketmq.tools.command.offset;

import java.lang.reflect.Field;
import java.util.Collections;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.MQClientAPIImpl;
import org.apache.rocketmq.client.impl.MQClientManager;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.admin.RollbackStats;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExtImpl;
import org.apache.rocketmq.tools.command.SubCommandException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SkipAccumulationCommandTest {
    private static DefaultMQAdminExt defaultMQAdminExt;
    private static DefaultMQAdminExtImpl defaultMQAdminExtImpl;
    private static MQClientInstance mqClientInstance = MQClientManager.getInstance().getOrCreateMQClientInstance(new ClientConfig());
    private static MQClientAPIImpl mQClientAPIImpl;

    @BeforeClass
    public static void init() throws Exception {
        mQClientAPIImpl = mock(MQClientAPIImpl.class);
        defaultMQAdminExt = new DefaultMQAdminExt();
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
    }

    @AfterClass
    public static void terminate() {
        defaultMQAdminExt.shutdown();
    }

    @Ignore
    @Test
    public void testExecute() throws SubCommandException {
        System.setProperty("rocketmq.namesrv.addr", "127.0.0.1:9876");
        SkipAccumulationSubCommand cmd = new SkipAccumulationSubCommand();
        Options options = ServerUtil.buildCommandlineOptions(new Options());
        String[] subargs = new String[] {"-g group-test", "-t topic-test", "-f false"};
        final CommandLine commandLine =
            ServerUtil.parseCmdLine("mqadmin " + cmd.commandName(), subargs,
                cmd.buildCommandlineOptions(options), new DefaultParser());
        cmd.execute(commandLine, options, null);
    }

    @Test
    public void testExecuteOfflineFallbackPreservesClusterName() throws Exception {
        DefaultMQAdminExt adminExt = mock(DefaultMQAdminExt.class);
        String clusterName = "LmqParentTopic";
        String topic = "%LMQ%topic-test";
        String group = "group-test";
        when(adminExt.resetOffsetByTimestamp(clusterName, topic, group, -1L, true))
            .thenThrow(new MQClientException(ResponseCode.CONSUMER_NOT_ONLINE, "consumer not online"));
        RollbackStats stats = new RollbackStats();
        stats.setBrokerName("broker-a");
        stats.setQueueId(0);
        when(adminExt.resetOffsetByTimestampOld(clusterName, group, topic, -1L, true))
            .thenReturn(Collections.singletonList(stats));

        SkipAccumulationSubCommand cmd = new SkipAccumulationSubCommand() {
            @Override
            DefaultMQAdminExt createDefaultMQAdminExt(RPCHook rpcHook) {
                return adminExt;
            }
        };
        Options options = ServerUtil.buildCommandlineOptions(new Options());
        String[] subargs = new String[] {"-g", group, "-t", topic, "-c", clusterName};
        CommandLine commandLine = ServerUtil.parseCmdLine("mqadmin " + cmd.commandName(), subargs,
            cmd.buildCommandlineOptions(options), new DefaultParser());

        cmd.execute(commandLine, options, null);

        verify(adminExt).resetOffsetByTimestampOld(clusterName, group, topic, -1L, true);
        verify(adminExt, never()).resetOffsetByTimestampOld(group, topic, -1L, true);
        verify(adminExt).shutdown();
    }

    @Test
    public void testExecuteOnlineWithClusterAndForceFalse() throws Exception {
        DefaultMQAdminExt adminExt = mock(DefaultMQAdminExt.class);
        String clusterName = "LmqParentTopic";
        String topic = "%LMQ%topic-test";
        String group = "group-test";
        when(adminExt.resetOffsetByTimestamp(clusterName, topic, group, -1L, false))
            .thenReturn(Collections.singletonMap(new MessageQueue(topic, "broker-a", 0), 100L));

        SkipAccumulationSubCommand cmd = new SkipAccumulationSubCommand() {
            @Override
            DefaultMQAdminExt createDefaultMQAdminExt(RPCHook rpcHook) {
                return adminExt;
            }
        };
        Options options = ServerUtil.buildCommandlineOptions(new Options());
        String[] subargs = new String[] {"-g", group, "-t", topic, "-c", clusterName, "-f", "false"};
        CommandLine commandLine = ServerUtil.parseCmdLine("mqadmin " + cmd.commandName(), subargs,
            cmd.buildCommandlineOptions(options), new DefaultParser());

        cmd.execute(commandLine, options, null);

        verify(adminExt).resetOffsetByTimestamp(clusterName, topic, group, -1L, false);
        verify(adminExt).shutdown();
    }
}
