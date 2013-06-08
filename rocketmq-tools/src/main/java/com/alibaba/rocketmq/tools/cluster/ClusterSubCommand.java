package com.alibaba.rocketmq.tools.cluster;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

import com.alibaba.rocketmq.tools.SubCommand;


/**
 * @author shijia.wxr<vintage.wang@gmail.com>
 */
public class ClusterSubCommand implements SubCommand {

    @Override
    public String commandName() {
        return "cluster";
    }


    @Override
    public String commandDesc() {
        return "List all of clusters or someone";
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
