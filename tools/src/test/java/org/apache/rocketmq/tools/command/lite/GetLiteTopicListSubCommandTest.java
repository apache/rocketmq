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

package org.apache.rocketmq.tools.command.lite;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.remoting.protocol.body.GetLiteTopicListResponseBody;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.apache.rocketmq.tools.command.MQAdminStartup;
import org.apache.rocketmq.tools.command.SubCommand;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class GetLiteTopicListSubCommandTest {

    private GetLiteTopicListResponseBody mockResponseBody() {
        GetLiteTopicListResponseBody responseBody = new GetLiteTopicListResponseBody();
        responseBody.setParentTopic("parentTopic");
        responseBody.setTotal(2);
        responseBody.setHasMore(true);

        GetLiteTopicListResponseBody.LiteTopicInfo first = new GetLiteTopicListResponseBody.LiteTopicInfo();
        first.setLiteTopic("order-1");
        first.setMinOffset(1);
        first.setMaxOffset(11);
        first.setLastUpdateTimestamp(System.currentTimeMillis());
        first.setSubscriberCount(2);
        first.setShardingToBroker(true);

        GetLiteTopicListResponseBody.LiteTopicInfo second = new GetLiteTopicListResponseBody.LiteTopicInfo();
        second.setLiteTopic("order-2");
        second.setMinOffset(0);
        second.setMaxOffset(3);
        second.setSubscriberCount(0);
        second.setShardingToBroker(false);
        responseBody.setLiteTopicList(Arrays.asList(first, second));
        return responseBody;
    }

    @Test
    public void testPrint() {
        GetLiteTopicListResponseBody responseBody = mockResponseBody();
        GetLiteTopicListSubCommand.printHeader();
        GetLiteTopicListSubCommand.printRow(responseBody, "broker-a");
        GetLiteTopicListSubCommand.printRow(responseBody, "broker-b");
        GetLiteTopicListSubCommand.printRow(null, "broker-a");
        GetLiteTopicListSubCommand.printRow(new GetLiteTopicListResponseBody(), "broker-a");
    }

    @Test
    public void testCommandOptions() {
        GetLiteTopicListSubCommand command = new GetLiteTopicListSubCommand();
        Options options = ServerUtil.buildCommandlineOptions(new Options());
        CommandLine commandLine = ServerUtil.parseCmdLine("mqadmin " + command.commandName(),
            new String[] {"-p", "parent-topic", "-x", "order-", "-m", "20"},
            command.buildCommandlineOptions(options), new DefaultParser());

        assertEquals("parent-topic", commandLine.getOptionValue('p'));
        assertEquals("order-", commandLine.getOptionValue('x'));
        assertEquals("20", commandLine.getOptionValue('m'));
        assertEquals("listLiteTopic", command.commandAlias());
    }

    @Test
    public void testCommandRegistration() throws Exception {
        MQAdminStartup.initCommand();

        // SUB_COMMANDS is intentionally package-private, so reach it reflectively instead of
        // widening production visibility just for this assertion.
        Field field = MQAdminStartup.class.getDeclaredField("SUB_COMMANDS");
        field.setAccessible(true);
        @SuppressWarnings("unchecked")
        List<SubCommand> subCommands = (List<SubCommand>) field.get(null);

        SubCommand registered = null;
        for (SubCommand command : subCommands) {
            if ("getLiteTopicList".equals(command.commandName())) {
                registered = command;
                break;
            }
        }
        assertTrue(registered instanceof GetLiteTopicListSubCommand);
    }
}
