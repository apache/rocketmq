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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.namesrv.NamesrvUtil;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.SubCommand;
import org.apache.rocketmq.tools.command.SubCommandException;

public class UpdateOrderConfCommand implements SubCommand {

    @Override
    public String commandName() {
        return "updateOrderConf";
    }

    @Override
    public String commandDesc() {
        return "Create or update or delete order conf.";
    }

    @Override
    public Options buildCommandlineOptions(Options options) {
        Option opt = new Option("t", "topic", true, "topic name");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("v", "orderConf", true, "set order conf [eg. brokerName1:num;brokerName2:num]");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("m", "method", true, "option type [eg. put|get|delete");
        opt.setRequired(true);
        options.addOption(opt);

        return options;
    }

    @Override
    public void execute(final CommandLine commandLine, final Options options,
        RPCHook rpcHook) throws SubCommandException {
        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt(rpcHook);
        defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));

        try {
            String topic = commandLine.getOptionValue('t').trim();
            String type = commandLine.getOptionValue('m').trim();

            if ("get".equals(type)) {

                defaultMQAdminExt.start();
                String orderConf =
                    defaultMQAdminExt.getKVConfig(NamesrvUtil.NAMESPACE_ORDER_TOPIC_CONFIG, topic);
                System.out.printf("get orderConf success. topic=[%s], orderConf=[%s] ", topic, orderConf);

                return;
            } else if ("put".equals(type)) {

                defaultMQAdminExt.start();
                String orderConf = "";
                if (commandLine.hasOption('v')) {
                    orderConf = commandLine.getOptionValue('v').trim();
                }
                if (UtilAll.isBlank(orderConf)) {
                    throw new Exception("please set orderConf with option -v.");
                }

                defaultMQAdminExt.createOrUpdateOrderConf(topic, orderConf, true);
                System.out.printf("update orderConf success. topic=[%s], orderConf=[%s]", topic,
                    orderConf.toString());
                return;
            } else if ("delete".equals(type)) {

                defaultMQAdminExt.start();
                defaultMQAdminExt.deleteKvConfig(NamesrvUtil.NAMESPACE_ORDER_TOPIC_CONFIG, topic);
                System.out.printf("delete orderConf success. topic=[%s]", topic);

                return;
            }

            ServerUtil.printCommandLineHelp("mqadmin " + this.commandName(), options);
        } catch (Exception e) {
            throw new SubCommandException(this.getClass().getSimpleName() + " command failed", e);
        } finally {
            defaultMQAdminExt.shutdown();
        }
    }
}
