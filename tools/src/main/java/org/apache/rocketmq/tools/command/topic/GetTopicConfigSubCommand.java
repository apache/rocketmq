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

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.protocol.body.TopicConfigSerializeWrapper;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.SubCommand;
import org.apache.rocketmq.tools.command.SubCommandException;

public class GetTopicConfigSubCommand implements SubCommand {
    @Override
    public String commandName() {
        return "getTopicConfig";
    }

    @Override
    public String commandDesc() {
        return "get topic config";
    }

    @Override
    public Options buildCommandlineOptions(Options options) {
        Option opt = new Option("b", "brokerAddr", true, "create topic to which broker");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("t", "topic", true, "topic name");
        opt.setRequired(true);
        options.addOption(opt);

        return options;
    }

    @Override
    public void execute(CommandLine commandLine, Options options,
        RPCHook rpcHook) throws SubCommandException {
        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt(rpcHook);
        defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));

        String addr = commandLine.getOptionValue('b').trim();
        String topic = commandLine.getOptionValue('t').trim();
        try {
            defaultMQAdminExt.start();

            TopicConfigSerializeWrapper wrapper = defaultMQAdminExt.getAllTopicConfig(addr, 10000L);
            TopicConfig topicConfig = wrapper.getTopicConfigTable().get(topic);
            if (topicConfig != null) {
                Field[] fields = topicConfig.getClass().getDeclaredFields();
                for (Field field : fields) {
                    if (Modifier.isStatic(field.getModifiers())) {
                        continue;
                    }
                    field.setAccessible(true);
                    if (field.get(topicConfig) != null) {
                        System.out.printf("%s%-64s=  %s\n", "", field.getName(), field.get(topicConfig).toString());
                    } else {
                        System.out.printf("%s%-64s=  %s\n", "", field.getName(), "");
                    }
                }
            }
        } catch (Exception e) {
            throw new SubCommandException(this.getClass().getSimpleName() + " command failed", e);
        } finally {
            defaultMQAdminExt.shutdown();
        }
    }
}
