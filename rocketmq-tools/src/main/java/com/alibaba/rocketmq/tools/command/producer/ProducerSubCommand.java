package com.alibaba.rocketmq.tools.command.producer;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

import com.alibaba.rocketmq.tools.command.SubCommand;


/**
 * @author shijia.wxr<vintage.wang@gmail.com>
 */
public class ProducerSubCommand implements SubCommand {

    @Override
    public String commandName() {
        return "producer";
    }


    @Override
    public String commandDesc() {
        return "Inspect data of producer";
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
