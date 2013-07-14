package com.alibaba.rocketmq.tools.command.stats;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

import com.alibaba.rocketmq.tools.command.SubCommand;


/**
 * @author shijia.wxr<vintage.wang@gmail.com>
 */
public class StatsSubCommand implements SubCommand {

    @Override
    public String commandName() {
        return "stats";
    }


    @Override
    public String commandDesc() {
        return "Print the stats of broker, producer or consumer";
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
