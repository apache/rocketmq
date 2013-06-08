package com.alibaba.rocketmq.tools.consumer;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

import com.alibaba.rocketmq.tools.SubCommand;


/**
 * @author shijia.wxr<vintage.wang@gmail.com>
 */
public class ConsumerSubCommand implements SubCommand {

    @Override
    public String commandName() {
        return "consumer";
    }


    @Override
    public String commandDesc() {
        return "Inspect data of consumer";
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
