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

package org.apache.rocketmq.tools.command.broker;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.SubCommand;
import org.apache.rocketmq.tools.command.SubCommandException;

/**
 * MQAdmin command which deletes expired CommitLog files
 */
public class DeleteExpiredCommitLogSubCommand implements SubCommand {

    @Override
    public String commandName() {
        return "deleteExpiredCommitLog";
    }

    @Override
    public String commandDesc() {
        return "Delete expired CommitLog files";
    }

    @Override
    public Options buildCommandlineOptions(Options options) {
        Option opt = new Option("n", "namesrvAddr", true, "Name server address");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("b", "brokerAddr", true, "Broker address");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("c", "cluster", true, "clustername");
        opt.setRequired(false);
        options.addOption(opt);

        return options;
    }

    @Override
    public void execute(CommandLine commandLine, Options options, RPCHook rpcHook) throws SubCommandException {
        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt(rpcHook);
        defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));
        if (commandLine.hasOption('n')) {
            defaultMQAdminExt.setNamesrvAddr(commandLine.getOptionValue('n').trim());
        }

        try {
            boolean result = false;
            defaultMQAdminExt.start();
            if (commandLine.hasOption('b')) {
                String addr = commandLine.getOptionValue('b').trim();
                result = defaultMQAdminExt.deleteExpiredCommitLogByAddr(addr);

            } else {
                String cluster = commandLine.getOptionValue('c');
                if (null != cluster)
                    cluster = cluster.trim();
                result = defaultMQAdminExt.deleteExpiredCommitLog(cluster);
            }
            System.out.printf(result ? "success" : "false");
        } catch (Exception e) {
            throw new SubCommandException(this.getClass().getSimpleName() + " command execute failed.", e);
        } finally {
            defaultMQAdminExt.shutdown();
        }
    }
}
