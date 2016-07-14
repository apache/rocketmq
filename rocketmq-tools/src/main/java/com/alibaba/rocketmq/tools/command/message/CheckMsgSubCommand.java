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

import com.alibaba.rocketmq.remoting.RPCHook;
import com.alibaba.rocketmq.tools.command.SubCommand;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;


/**
 * @auther lansheng.zj
 */
public class CheckMsgSubCommand implements SubCommand {
    @Override
    public String commandName() {
        return "checkMsg";
    }


    @Override
    public String commandDesc() {
        return "Check Message Store";
    }


    @Override
    public Options buildCommandlineOptions(Options options) {
        Option opt = new Option("p", "cStorePath", true, "cStorePath");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("s", "cSize ", true, "cSize");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("l", "lStorePath ", true, "lStorePath");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("z", "lSize ", true, "lSize");
        opt.setRequired(true);
        options.addOption(opt);

        return options;
    }


    @Override
    public void execute(CommandLine commandLine, Options options, RPCHook rpcHook) {
        Store store = new Store(commandLine.getOptionValue("cStorePath").trim(), //
                Integer.parseInt(commandLine.getOptionValue("cSize").trim()),//
                commandLine.getOptionValue("lStorePath").trim(), //
                Integer.parseInt(commandLine.getOptionValue("lSize").trim()));
        store.load();
        store.traval(false);
    }
}
