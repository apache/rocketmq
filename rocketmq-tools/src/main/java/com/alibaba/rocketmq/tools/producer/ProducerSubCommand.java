package com.alibaba.rocketmq.tools.producer;

import com.alibaba.rocketmq.tools.SubCommand;


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
        return "Insepect data of producer";
    }


    @Override
    public void printHelp() {
    }


    @Override
    public void execute(String[] args) {
    }
}
