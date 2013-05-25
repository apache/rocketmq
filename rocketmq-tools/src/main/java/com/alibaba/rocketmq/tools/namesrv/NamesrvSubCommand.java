package com.alibaba.rocketmq.tools.namesrv;

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
    public void printHelp() {
    }


    @Override
    public void execute(String[] args) {
        // TODO Auto-generated method stub

    }

}
