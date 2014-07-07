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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import com.alibaba.rocketmq.common.MQVersion;
import com.alibaba.rocketmq.common.protocol.body.Connection;
import com.alibaba.rocketmq.common.protocol.body.ProducerConnection;
import com.alibaba.rocketmq.remoting.RPCHook;
import com.alibaba.rocketmq.tools.admin.DefaultMQAdminExt;
import com.alibaba.rocketmq.tools.command.SubCommand;


/**
 * 查询Producer的网络连接
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-10-13
 */
public class ProducerConnectionSubCommand implements SubCommand {

    @Override
    public String commandName() {
        return "producerConnection";
    }


    @Override
    public String commandDesc() {
        return "Query producer's socket connection and client version";
    }


    @Override
    public Options buildCommandlineOptions(Options options) {
        Option opt = new Option("g", "producerGroup", true, "producer group name");
        opt.setRequired(true);
        options.addOption(opt);

        // topic必须设置
        opt = new Option("t", "topic", true, "topic name");
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
            String topic = commandLine.getOptionValue('t').trim();

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
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            defaultMQAdminExt.shutdown();
        }
    }
}
