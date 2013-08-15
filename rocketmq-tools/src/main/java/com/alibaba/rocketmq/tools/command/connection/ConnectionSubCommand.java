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
package com.alibaba.rocketmq.tools.command.connection;

import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import com.alibaba.rocketmq.common.MQVersion;
import com.alibaba.rocketmq.common.MixAll;
import com.alibaba.rocketmq.common.protocol.body.Connection;
import com.alibaba.rocketmq.common.protocol.body.ConsumerConnection;
import com.alibaba.rocketmq.common.protocol.body.ProducerConnection;
import com.alibaba.rocketmq.common.protocol.heartbeat.SubscriptionData;
import com.alibaba.rocketmq.tools.admin.DefaultMQAdminExt;
import com.alibaba.rocketmq.tools.command.SubCommand;


/**
 * 连接查询工具
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-8-5
 */
public class ConnectionSubCommand implements SubCommand {

    @Override
    public String commandName() {
        return "connectionList";
    }


    @Override
    public String commandDesc() {
        return "Fetch producer or consumer socket connection";
    }


    @Override
    public Options buildCommandlineOptions(Options options) {
        Option opt = new Option("p", "producerGroup", true, "producer group name");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("c", "consumerGroup", true, "consumer group name");
        opt.setRequired(false);
        options.addOption(opt);

        // topic必须设置
        opt = new Option("t", "topic", true, "topic name");
        opt.setRequired(false);
        options.addOption(opt);

        return options;
    }


    @Override
    public void execute(CommandLine commandLine, Options options) {
        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt();

        defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));

        try {

            // Producer
            if (commandLine.hasOption('p') && commandLine.hasOption('t')) {
                defaultMQAdminExt.start();

                String topic = commandLine.getOptionValue('t');

                String group = commandLine.getOptionValue('p');

                ProducerConnection pc = defaultMQAdminExt.examineProducerConnectionInfo(group, topic);

                int i = 1;
                for (Connection conn : pc.getConnectionSet()) {
                    System.out.printf("%04d  %-32s %-22s %-8s %s\n",//
                        i++,//
                        conn.getClientId(),//
                        conn.getClientAddr(),//
                        conn.getLanguage(),//
                        MQVersion.getVersionDesc(conn.getVersion())//
                        );
                }

                return;
            }
            // Consumer
            else if (commandLine.hasOption('c')) {
                defaultMQAdminExt.start();

                String group = commandLine.getOptionValue('c');

                ConsumerConnection cc = defaultMQAdminExt.examineConsumerConnectionInfo(group);

                // 打印连接
                int i = 1;
                for (Connection conn : cc.getConnectionSet()) {
                    System.out.printf("%03d  %-32s %-22s %-8s %s\n",//
                        i++,//
                        conn.getClientId(),//
                        conn.getClientAddr(),//
                        conn.getLanguage(),//
                        MQVersion.getVersionDesc(conn.getVersion())//
                        );
                }

                // 打印订阅关系
                System.out.println("\nBelow is subscription:");
                Iterator<Entry<String, SubscriptionData>> it =
                        cc.getSubscriptionTable().entrySet().iterator();
                i = 1;
                while (it.hasNext()) {
                    Entry<String, SubscriptionData> entry = it.next();
                    SubscriptionData sd = entry.getValue();
                    System.out.printf("%03d  Topic: %-40s SubExpression: %s\n",//
                        i++,//
                        sd.getTopic(),//
                        sd.getSubString()//
                        );
                }

                // 打印其他订阅参数
                System.out.println("");
                System.out.printf("ConsumeType: %s\n", cc.getConsumeType());
                System.out.printf("MessageModel: %s\n", cc.getMessageModel());
                System.out.printf("ConsumeFromWhere: %s\n", cc.getConsumeFromWhere());
                return;
            }

            MixAll.printCommandLineHelp("mqadmin " + this.commandName(), options);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            defaultMQAdminExt.shutdown();
        }
    }
}
