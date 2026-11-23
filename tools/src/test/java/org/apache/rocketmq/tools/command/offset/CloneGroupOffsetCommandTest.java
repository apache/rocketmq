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

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.netty.NettyRemotingServer;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.admin.ConsumeStats;
import org.apache.rocketmq.remoting.protocol.admin.OffsetWrapper;
import org.apache.rocketmq.remoting.protocol.header.GetTopicConfigRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.UpdateConsumerOffsetRequestHeader;
import org.apache.rocketmq.remoting.protocol.statictopic.TopicConfigAndQueueMapping;
import org.apache.rocketmq.tools.command.server.NameServerMocker;
import org.apache.rocketmq.tools.command.server.ServerResponseMocker;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

public class CloneGroupOffsetCommandTest {
    @Test
    public void testExecuteClonesOnlySelectedTopic() throws Exception {
        OffsetWrapper selectedOffset = new OffsetWrapper();
        selectedOffset.setConsumerOffset(42L);
        OffsetWrapper otherOffset = new OffsetWrapper();
        otherOffset.setConsumerOffset(7L);
        ConsumeStats stats = new ConsumeStats();
        stats.getOffsetTable().put(new MessageQueue("TopicA", "mockBrokerName", 0), selectedOffset);
        stats.getOffsetTable().put(new MessageQueue("TopicB", "mockBrokerName", 0), otherOffset);

        List<UpdateConsumerOffsetRequestHeader> updates = new CopyOnWriteArrayList<>();
        NettyServerConfig config = new NettyServerConfig();
        config.setBindAddress("127.0.0.1");
        config.setListenPort(0);
        config.setServerSelectorThreads(1);
        config.setServerWorkerThreads(1);
        config.setServerCallbackExecutorThreads(1);
        NettyRemotingServer broker = new NettyRemotingServer(config);
        broker.registerProcessor(RequestCode.GET_CONSUME_STATS, (ctx, request) -> success(stats.encode()), null);
        broker.registerProcessor(RequestCode.GET_TOPIC_CONFIG, (ctx, request) -> {
            GetTopicConfigRequestHeader header = request.decodeCommandCustomHeader(GetTopicConfigRequestHeader.class);
            TopicConfigAndQueueMapping topicConfig = new TopicConfigAndQueueMapping(new TopicConfig(header.getTopic()), null);
            return success(RemotingSerializable.encode(topicConfig));
        }, null);
        broker.registerProcessor(RequestCode.UPDATE_CONSUMER_OFFSET, (ctx, request) -> {
            updates.add(request.decodeCommandCustomHeader(UpdateConsumerOffsetRequestHeader.class));
            return success(null);
        }, null);

        String originalNamesrvAddr = System.getProperty(MixAll.NAMESRV_ADDR_PROPERTY);
        ServerResponseMocker nameServer = null;
        try {
            broker.start();
            nameServer = NameServerMocker.startByDefaultConf(broker.localListenPort());
            System.setProperty(MixAll.NAMESRV_ADDR_PROPERTY, "127.0.0.1:" + nameServer.listenPort());
            CloneGroupOffsetCommand command = new CloneGroupOffsetCommand();
            Options options = command.buildCommandlineOptions(new Options());
            CommandLine commandLine = new DefaultParser().parse(options,
                new String[] {"-s", "source-group", "-d", "target-group", "-t", "TopicA"});
            command.execute(commandLine, options, null);
        } finally {
            if (originalNamesrvAddr == null) {
                System.clearProperty(MixAll.NAMESRV_ADDR_PROPERTY);
            } else {
                System.setProperty(MixAll.NAMESRV_ADDR_PROPERTY, originalNamesrvAddr);
            }
            if (nameServer != null) {
                nameServer.shutdown();
            }
            broker.shutdown();
        }

        assertThat(updates).extracting(UpdateConsumerOffsetRequestHeader::getConsumerGroup,
            UpdateConsumerOffsetRequestHeader::getTopic, UpdateConsumerOffsetRequestHeader::getQueueId,
            UpdateConsumerOffsetRequestHeader::getCommitOffset).containsExactly(tuple("target-group", "TopicA", 0, 42L));
    }

    private static RemotingCommand success(byte[] body) {
        RemotingCommand response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);
        response.setBody(body);
        return response;
    }
}
