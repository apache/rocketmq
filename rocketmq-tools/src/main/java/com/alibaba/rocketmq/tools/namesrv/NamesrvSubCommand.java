package com.alibaba.rocketmq.tools.namesrv;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

import com.alibaba.rocketmq.tools.SubCommand;


/**
 * @author shijia.wxr<vintage.wang@gmail.com>
 */
public class NamesrvSubCommand implements SubCommand {

    @Override
    public String commandName() {
        return "namesrv";
    }


    @Override
    public String commandDesc() {
        return "List the name servers and manage config of the name server";
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
