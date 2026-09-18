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

package org.apache.rocketmq.namesrv.processor;

import io.netty.channel.Channel;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.namesrv.NamesrvConfig;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.body.TopicConfigSerializeWrapper;
import org.apache.rocketmq.remoting.protocol.body.TopicList;
import org.apache.rocketmq.namesrv.NamesrvController;
import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

public class DefaultRequestProcessorGetTopicsByClusterTest {

    private DefaultRequestProcessor defaultRequestProcessor;

    @Before
    public void init() {
        NamesrvController namesrvController = new NamesrvController(new NamesrvConfig(), new NettyServerConfig());
        defaultRequestProcessor = new DefaultRequestProcessor(namesrvController);

        TopicConfigSerializeWrapper topicConfigSerializeWrapper = new TopicConfigSerializeWrapper();
        ConcurrentHashMap<String, TopicConfig> topicConfigTable = new ConcurrentHashMap<>();
        for (int i = 0; i < 2; i++) {
            TopicConfig topicConfig = new TopicConfig("unit-test" + i);
            topicConfigTable.put(topicConfig.getTopicName(), topicConfig);
        }
        topicConfigSerializeWrapper.setTopicConfigTable(topicConfigTable);
        namesrvController.getRouteInfoManager().registerBroker("default-cluster", "127.0.0.1:10911", "default-broker",
            1234, "127.0.0.1:1001", "", null, topicConfigSerializeWrapper, new ArrayList<>(), mock(Channel.class));
    }

    @Test
    public void testGetTopicsByCluster() throws Exception {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_TOPICS_BY_CLUSTER, null);
        request.addExtField("cluster", "default-cluster");

        RemotingCommand response = defaultRequestProcessor.processRequest(null, request);

        assertThat(response.getCode()).isEqualTo(ResponseCode.SUCCESS);
        assertThat(TopicList.decode(response.getBody(), TopicList.class).getTopicList())
            .containsExactlyInAnyOrder("unit-test0", "unit-test1");
    }

    @Test
    public void testGetTopicsByClusterNotExisted() throws Exception {
        // An unknown cluster used to be reported as an empty success, which
        // clients cannot distinguish from a cluster that simply has no topics.
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_TOPICS_BY_CLUSTER, null);
        request.addExtField("cluster", "not-exist-cluster");

        RemotingCommand response = defaultRequestProcessor.processRequest(null, request);

        assertThat(response.getCode()).isEqualTo(ResponseCode.SYSTEM_ERROR);
        assertThat(response.getRemark()).contains("not-exist-cluster");
    }
}
