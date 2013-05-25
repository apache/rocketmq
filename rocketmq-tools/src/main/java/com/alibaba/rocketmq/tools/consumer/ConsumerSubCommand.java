package com.alibaba.rocketmq.tools.consumer;

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
    public void printHelp() {
    }


    @Override
    public void execute(String[] args) {
    }
}
