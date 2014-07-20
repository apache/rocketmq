/**
 * Copyright (C) 2010-2013 Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.rocketmq.tools.command.consumer;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import com.alibaba.rocketmq.common.protocol.body.Connection;
import com.alibaba.rocketmq.common.protocol.body.ConsumerConnection;
import com.alibaba.rocketmq.common.protocol.body.ConsumerRunningInfo;
import com.alibaba.rocketmq.tools.admin.DefaultMQAdminExt;
import com.alibaba.rocketmq.tools.command.SubCommand;


/**
 * 查询Consumer内部数据结构
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2014-07-20
 */
public class ConsumerStatusSubCommand implements SubCommand {

    @Override
    public String commandName() {
        return "consumerStatus";
    }


    @Override
    public String commandDesc() {
        return "Query consumer's internal data structure";
    }


    @Override
    public Options buildCommandlineOptions(Options options) {
        Option opt = new Option("g", "consumerGroup", true, "consumer group name");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("i", "clientId", true, "The consumer's client id");
        opt.setRequired(false);
        options.addOption(opt);

        return options;
    }


    @Override
    public void execute(CommandLine commandLine, Options options) {
        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt();

        defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));

        try {
            defaultMQAdminExt.start();

            String group = commandLine.getOptionValue('g').trim();

            ConsumerConnection cc = defaultMQAdminExt.examineConsumerConnectionInfo(group);

            if (!commandLine.hasOption('i')) {
                // 打印连接
                int i = 1;
                for (Connection conn : cc.getConnectionSet()) {
                    System.out.printf("%03d  %-32s\n",//
                        i++,//
                        conn.getClientId()//
                        );
                }
            }
            else {
                String clientId = commandLine.getOptionValue('i').trim();
                ConsumerRunningInfo consumerRunningInfo =
                        defaultMQAdminExt.getConsumerRunningInfo(group, clientId);
                if (consumerRunningInfo != null) {
                    System.out.println(consumerRunningInfo.formatString());
                }
            }

        }
        catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            defaultMQAdminExt.shutdown();
        }
    }
}
