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

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.tools.command.server.ServerResponseMocker;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ExportMetadataCommandTest {
    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Test
    public void testBrokerExportIncludesTopicsAndSubscriptionGroupsByDefault() throws Exception {
        TopicConfig topic = new TopicConfig("example-topic");
        topic.setWriteQueueNums(4);
        SubscriptionGroupConfig group = new SubscriptionGroupConfig();
        group.setGroupName("example-group");

        // The mock can return a single payload to each RPC because the response types read distinct fields.
        JSONObject response = new JSONObject();
        response.put("topicConfigTable", Collections.singletonMap(topic.getTopicName(), topic));
        response.put("subscriptionGroupTable", Collections.singletonMap(group.getGroupName(), group));
        response.put("topicList", Collections.emptyList());
        ServerResponseMocker broker = ServerResponseMocker.startServer(JSON.toJSONBytes(response));
        String address = "127.0.0.1:" + broker.listenPort();
        String previousNamesrv = System.setProperty(MixAll.NAMESRV_ADDR_PROPERTY, address);

        try {
            Path output = tempFolder.newFolder().toPath();
            ExportMetadataCommand command = new ExportMetadataCommand();
            Options options = command.buildCommandlineOptions(new Options());
            CommandLine args = new DefaultParser().parse(options, new String[] {"-b", address, "-f", output.toString()});
            command.execute(args, options, null);

            Path file = output.resolve("metadata.json");
            assertTrue("A broker-only export should write metadata.json", Files.isRegularFile(file));
            JSONObject metadata = JSON.parseObject(Files.readAllBytes(file));
            JSONObject exportedTopic = metadata.getJSONObject("topicConfigTable").getJSONObject(topic.getTopicName());
            assertEquals(4, exportedTopic.getIntValue("writeQueueNums"));
            JSONObject exportedGroup = metadata.getJSONObject("subscriptionGroupTable").getJSONObject(group.getGroupName());
            assertEquals(group.getGroupName(), exportedGroup.getString("groupName"));
            assertTrue(metadata.getLongValue("exportTime") > 0);
        } finally {
            if (previousNamesrv == null) {
                System.clearProperty(MixAll.NAMESRV_ADDR_PROPERTY);
            } else {
                System.setProperty(MixAll.NAMESRV_ADDR_PROPERTY, previousNamesrv);
            }
            broker.shutdown();
        }
    }
}
