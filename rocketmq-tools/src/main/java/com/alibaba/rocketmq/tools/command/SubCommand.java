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
package com.alibaba.rocketmq.tools.command;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

import com.alibaba.rocketmq.remoting.RPCHook;


/**
 * 各个子命令的接口
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-7-25
 */
public interface SubCommand {
    public String commandName();


    public String commandDesc();


    public Options buildCommandlineOptions(final Options options);


    public void execute(final CommandLine commandLine, final Options options, RPCHook rpcHook);
}
