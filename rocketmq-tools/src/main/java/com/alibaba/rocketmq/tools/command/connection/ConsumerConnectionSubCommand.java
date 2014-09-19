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
import com.alibaba.rocketmq.common.protocol.body.Connection;
import com.alibaba.rocketmq.common.protocol.body.ConsumerConnection;
import com.alibaba.rocketmq.common.protocol.heartbeat.SubscriptionData;
import com.alibaba.rocketmq.remoting.RPCHook;
import com.alibaba.rocketmq.tools.admin.DefaultMQAdminExt;
import com.alibaba.rocketmq.tools.command.SubCommand;


/**
 * 查询Consumer的网络连接，以及客户端版本号，订阅关系等
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-10-13
 */
public class ConsumerConnectionSubCommand implements SubCommand {

    @Override
    public String commandName() {
        return "consumerConnection";
    }


    @Override
    public String commandDesc() {
        return "Query consumer's socket connection, client version and subscription";
    }


    @Override
    public Options buildCommandlineOptions(Options options) {
        Option opt = new Option("g", "consumerGroup", true, "consumer group name");
        opt.setRequired(true);
        options.addOption(opt);

        return options;
    }


    @Override
    public void execute(CommandLine commandLine, Options options, RPCHook rpcHook) {
        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt(rpcHook);

        defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));

        try {
            defaultMQAdminExt.start();

            String group = commandLine.getOptionValue('g').trim();

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
            Iterator<Entry<String, SubscriptionData>> it = cc.getSubscriptionTable().entrySet().iterator();
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
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            defaultMQAdminExt.shutdown();
        }
    }
}
