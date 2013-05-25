package com.alibaba.rocketmq.tools.message;

import com.alibaba.rocketmq.tools.SubCommand;


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
    public void printHelp() {
        // TODO Auto-generated method stub

    }


    @Override
    public void execute(String[] args) {
        // TODO Auto-generated method stub

    }
}
