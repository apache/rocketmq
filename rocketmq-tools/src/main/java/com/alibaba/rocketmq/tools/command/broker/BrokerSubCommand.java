/**
 * 
 */
package com.alibaba.rocketmq.tools.command.broker;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

import com.alibaba.rocketmq.tools.command.SubCommand;


/**
 * @author shijia.wxr<vintage.wang@gmail.com>
 */
public class BrokerSubCommand implements SubCommand {

    @Override
    public String commandName() {
        return "broker";
    }


    @Override
    public String commandDesc() {
        return "Inspect the broker's data and update";
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
