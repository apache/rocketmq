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
package org.apache.rocketmq.tools.command.export;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.rocketmq.common.protocol.body.ClusterInfo;
import org.apache.rocketmq.common.protocol.body.SubscriptionGroupWrapper;
import org.apache.rocketmq.common.protocol.body.TopicConfigSerializeWrapper;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.apache.rocketmq.tools.command.SubCommandTest;
import org.apache.rocketmq.tools.util.MockObjectUtil;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.powermock.api.mockito.PowerMockito.doReturn;

@RunWith(PowerMockRunner.class)
@PrepareForTest(ExportMetadataCommand.class)
public class ExportMetadataCommandTest extends SubCommandTest {
    @Test
    public void testExportMetadata() throws Exception {
        {
            TopicConfigSerializeWrapper topicConfigWrapper = MockObjectUtil.createTopicConfigWrapper();
            doReturn(topicConfigWrapper).when(defaultMQAdminExt).getUserTopicConfig(anyString(), anyBoolean(), anyLong());
            SubscriptionGroupWrapper subscriptionGroupWrapper = MockObjectUtil.createSubscriptionGroupWrapper();
            doReturn(subscriptionGroupWrapper).when(defaultMQAdminExt).getUserSubscriptionGroup(anyString(), anyLong());
        }

        ExportMetadataCommand cmd = new ExportMetadataCommand();
        Options options = ServerUtil.buildCommandlineOptions(new Options());
        // eg: sh mqadmin exportMetadata -b 127.0.0.1:10911 -t -g
        String[] subargsWithBrokerAddr = new String[] {
            "-b 127.0.0.1:10911",
            "-t",
            "-g"};
        CommandLine commandLine =
            ServerUtil.parseCmdLine("mqadmin " + cmd.commandName(), subargsWithBrokerAddr, cmd.buildCommandlineOptions(options), new PosixParser());
        cmd.execute(commandLine, options, rpcHook);

        {
            ClusterInfo clusterInfo = MockObjectUtil.createClusterInfo();
            doReturn(clusterInfo).when(defaultMQAdminExt).examineBrokerClusterInfo();
        }
        // eg: sh mqadmin exportMetadata -c DefaultCluster -t -g
        String[] subargsWithClusterNameAndTopic = new String[] {
            "-c DefaultCluster",
            "-t",
            "-g"};
        commandLine =
            ServerUtil.parseCmdLine("mqadmin " + cmd.commandName(), subargsWithClusterNameAndTopic, cmd.buildCommandlineOptions(options), new PosixParser());
        cmd.execute(commandLine, options, rpcHook);

        // eg: sh mqadmin exportMetadata -c DefaultCluster
        String[] subargsWithClusterName = new String[] {"-c DefaultCluster",};
        commandLine =
            ServerUtil.parseCmdLine("mqadmin " + cmd.commandName(), subargsWithClusterName, cmd.buildCommandlineOptions(options), new PosixParser());
        cmd.execute(commandLine, options, rpcHook);
    }
}
