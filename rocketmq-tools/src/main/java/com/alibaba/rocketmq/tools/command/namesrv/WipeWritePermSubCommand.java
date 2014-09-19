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
package com.alibaba.rocketmq.tools.command.namesrv;

import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import com.alibaba.rocketmq.remoting.RPCHook;
import com.alibaba.rocketmq.tools.admin.DefaultMQAdminExt;
import com.alibaba.rocketmq.tools.command.SubCommand;


/**
 * 从所有Name Server上清除特定Broker的先权限
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-8-6
 */
public class WipeWritePermSubCommand implements SubCommand {

    @Override
    public String commandName() {
        return "wipeWritePerm";
    }


    @Override
    public String commandDesc() {
        return "Wipe write perm of broker in all name server";
    }


    @Override
    public Options buildCommandlineOptions(Options options) {
        Option opt = new Option("b", "brokerName", true, "broker name");
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
            String brokerName = commandLine.getOptionValue('b').trim();
            List<String> namesrvList = defaultMQAdminExt.getNameServerAddressList();
            if (namesrvList != null) {
                for (String namesrvAddr : namesrvList) {
                    try {
                        int wipeTopicCount = defaultMQAdminExt.wipeWritePermOfBroker(namesrvAddr, brokerName);
                        System.out.printf("wipe write perm of broker[%s] in name server[%s] OK, %d\n",//
                            brokerName,//
                            namesrvAddr,//
                            wipeTopicCount//
                            );
                    }
                    catch (Exception e) {
                        System.out.printf("wipe write perm of broker[%s] in name server[%s] Failed\n",//
                            brokerName,//
                            namesrvAddr//
                            );

                        e.printStackTrace();
                    }
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
