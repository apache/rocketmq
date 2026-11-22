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

import java.util.Collections;
import java.util.HashMap;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.protocol.admin.ConsumeStats;
import org.apache.rocketmq.remoting.protocol.admin.OffsetWrapper;
import org.apache.rocketmq.remoting.protocol.route.BrokerData;
import org.apache.rocketmq.remoting.protocol.route.TopicRouteData;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.junit.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class CloneGroupOffsetCommandTest {
    @Test
    public void testExecuteClonesOnlySelectedTopic() throws Exception {
        String brokerName = "broker-a";
        String brokerAddress = "127.0.0.1:10911";
        MessageQueue selectedQueue = new MessageQueue("TopicA", brokerName, 0);
        MessageQueue otherQueue = new MessageQueue("TopicB", brokerName, 0);

        OffsetWrapper selectedOffset = new OffsetWrapper();
        selectedOffset.setConsumerOffset(42L);
        OffsetWrapper otherOffset = new OffsetWrapper();
        otherOffset.setConsumerOffset(7L);
        ConsumeStats stats = new ConsumeStats();
        stats.getOffsetTable().put(selectedQueue, selectedOffset);
        stats.getOffsetTable().put(otherQueue, otherOffset);

        TopicRouteData route = new TopicRouteData();
        route.setBrokerDatas(Collections.singletonList(new BrokerData("cluster", brokerName,
            new HashMap<>(Collections.singletonMap(MixAll.MASTER_ID, brokerAddress)))));

        DefaultMQAdminExt admin = mock(DefaultMQAdminExt.class);
        when(admin.examineConsumeStats("source-group")).thenReturn(stats);
        when(admin.examineTopicRouteInfo("TopicA")).thenReturn(route);
        CloneGroupOffsetCommand command = new CloneGroupOffsetCommand() {
            @Override
            DefaultMQAdminExt createAdminExt(RPCHook rpcHook) {
                return admin;
            }
        };

        Options options = command.buildCommandlineOptions(new Options());
        CommandLine commandLine = new DefaultParser().parse(options,
            new String[] {"-s", "source-group", "-d", "target-group", "-t", "TopicA"});
        command.execute(commandLine, options, null);

        verify(admin).updateConsumeOffset(brokerAddress, "target-group", selectedQueue, 42L);
        verify(admin, never()).updateConsumeOffset(any(), any(), eq(otherQueue), anyLong());
    }
}
