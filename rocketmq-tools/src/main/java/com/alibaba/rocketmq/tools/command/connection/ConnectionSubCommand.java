/**
 * 
 */
package com.alibaba.rocketmq.tools.command.connection;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

import com.alibaba.rocketmq.tools.command.SubCommand;


/**
 * @author shijia.wxr<vintage.wang@gmail.com>
 */
public class ConnectionSubCommand implements SubCommand {

    @Override
    public String commandName() {
        return "connection";
    }


    @Override
    public String commandDesc() {
        return "List connections of producer or consumer";
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
