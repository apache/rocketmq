/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.alibaba.rocketmq.tools.command.message;

import com.alibaba.rocketmq.common.UtilAll;
import com.alibaba.rocketmq.common.message.MessageClientIDSetter;
import com.alibaba.rocketmq.remoting.RPCHook;
import com.alibaba.rocketmq.tools.command.SubCommand;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

/**
 * Created by jodie on 16/8/18.
 */
public class DecodeMessageIdCommond implements SubCommand {
    @Override
    public String commandName() {
        return "DecodeMessageId";
    }


    @Override
    public String commandDesc() {
        return "decode unique message ID";
    }


    @Override
    public Options buildCommandlineOptions(Options options) {
        Option opt = new Option("i", "messageId", true, "unique message ID");
        opt.setRequired(false);
        options.addOption(opt);
        return options;
    }


    @Override
    public void execute(final CommandLine commandLine, final Options options, RPCHook rpcHook) {
        String messageId = commandLine.getOptionValue('i').trim();

        try {
            System.out.println("ip=" + MessageClientIDSetter.getIPStrFromID(messageId));
        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            String date = UtilAll.formatDate(MessageClientIDSetter.getNearlyTimeFromID(messageId), UtilAll.yyyy_MM_dd_HH_mm_ss_SSS);
            System.out.println("date=" + date);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        System.out.println(MessageClientIDSetter.getIPStrFromID("783786393794180EEDC3636C5D3A0011"));
        System.out.println(MessageClientIDSetter.getIPStrFromID("783786393794180EEDC3636C5D420013"));
        System.out.println(MessageClientIDSetter.getIPStrFromID("783786393794180EEDC36386D07B001F"));
    }
}
