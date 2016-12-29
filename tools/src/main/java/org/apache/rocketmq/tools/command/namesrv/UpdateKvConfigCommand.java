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
package org.apache.rocketmq.tools.command.namesrv;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.SubCommand;

public class UpdateKvConfigCommand implements SubCommand {
    @Override
    public String commandName() {
        return "updateKvConfig";
    }

    @Override
    public String commandDesc() {
        return "Create or update KV config.";
    }

    @Override
    public Options buildCommandlineOptions(Options options) {
        Option opt = new Option("s", "namespace", true, "set the namespace");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("k", "key", true, "set the key name");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("v", "value", true, "set the key value");
        opt.setRequired(true);
        options.addOption(opt);
        return options;
    }

    @Override
    public void execute(CommandLine commandLine, Options options, RPCHook rpcHook) {
        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt(rpcHook);
        defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));
        try {
            // namespace
            String namespace = commandLine.getOptionValue('s').trim();
            // key name
            String key = commandLine.getOptionValue('k').trim();
            // key name
            String value = commandLine.getOptionValue('v').trim();

            defaultMQAdminExt.start();
            defaultMQAdminExt.createAndUpdateKvConfig(namespace, key, value);
            System.out.printf("create or update kv config to namespace success.%n");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            defaultMQAdminExt.shutdown();
        }
    }
}
