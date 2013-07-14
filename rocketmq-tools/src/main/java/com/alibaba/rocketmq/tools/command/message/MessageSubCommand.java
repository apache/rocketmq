package com.alibaba.rocketmq.tools.command.message;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

import com.alibaba.rocketmq.tools.command.SubCommand;


/**
 * @author shijia.wxr<vintage.wang@gmail.com>
 */
public class MessageSubCommand implements SubCommand {

    @Override
    public String commandName() {
        return "message";
    }


    @Override
    public String commandDesc() {
        return "Query message by id or by key";
    }


    @Override
    public Options buildCommandlineOptions(Options options) {
        // TODO Auto-generated method stub
        return null;
    }


    @Override
    public void execute(CommandLine commandLine) {
        // TODO Auto-generated method stub

    }
}
