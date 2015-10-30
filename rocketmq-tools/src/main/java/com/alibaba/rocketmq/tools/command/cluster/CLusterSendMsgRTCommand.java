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

package com.alibaba.rocketmq.tools.command.cluster;

import java.util.Set;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.common.message.Message;
import com.alibaba.rocketmq.common.protocol.body.ClusterInfo;
import com.alibaba.rocketmq.remoting.RPCHook;
import com.alibaba.rocketmq.tools.admin.DefaultMQAdminExt;
import com.alibaba.rocketmq.tools.command.SubCommand;

/**
 * 查看集群信息
 * 
 * @author fengliang.hfl
 * @since 2015-10-30
 */
public class CLusterSendMsgRTCommand implements SubCommand {

    @Override
    public String commandName() {
        return "clusterRT";
    }

    @Override
    public String commandDesc() {
        return "List All clusters Message Send RT";
    }

    @Override
    public Options buildCommandlineOptions(Options options) {
        Option opt = new Option("a", "amout", true, "message amout | default 100");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("s", "size", true, "message size | default 128 Byte");
        opt.setRequired(true);
        options.addOption(opt);

        return options;
    }

    @Override
    public void execute(CommandLine commandLine, Options options, RPCHook rpcHook) {
        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt(rpcHook);
        defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));

        DefaultMQProducer producer = new DefaultMQProducer(rpcHook);
        producer.setProducerGroup(Long.toString(System.currentTimeMillis()));

        try {
            defaultMQAdminExt.start();
            producer.start();

            ClusterInfo clusterInfoSerializeWrapper = defaultMQAdminExt.examineBrokerClusterInfo();
            Set<String> clusterNames = clusterInfoSerializeWrapper.getClusterAddrTable().keySet();

            long amount = !commandLine.hasOption('a') ? 50 : Long.parseLong(commandLine
                    .getOptionValue('a').trim());

            long size = !commandLine.hasOption('s') ? 128 : Long.parseLong(commandLine
                    .getOptionValue('s').trim());

            System.out.printf("%-24s  %-4s  %-8s  %-8s\n",//
                    "#Cluster Name",//
                    "#RT",//
                    "#successCount",//
                    "#failCount"//
            );

            for (String clusterName : clusterNames) {
                Message msg = new Message(clusterName, getStringBySize(size).getBytes());

                long start = 0;
                long end = 0;
                long elapsed = 0;
                int successCount = 0;
                int failCount = 0;

                for (int i = 0; i < amount; i++) {
                    start = System.currentTimeMillis();
                    try {
                        producer.send(msg);
                        successCount++;
                        end = System.currentTimeMillis();
                    } catch (Exception e) {
                        failCount++;
                        end = System.currentTimeMillis();
                    }

                    if (i != 0) {
                        elapsed += end - start;
                    }
                }

                System.out.printf("%-24s  %-4s  %-16s  %-8s\n",//
                        clusterName,//
                        elapsed / (amount - 1),//
                        successCount,//
                        failCount//
                        );
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            defaultMQAdminExt.shutdown();
            producer.shutdown();
        }
    }

    public String getStringBySize(long size) {
        StringBuilder res = new StringBuilder();
        for (int i = 0; i < size; i++) {
            res.append('a');
        }
        return res.toString();
    }

}
