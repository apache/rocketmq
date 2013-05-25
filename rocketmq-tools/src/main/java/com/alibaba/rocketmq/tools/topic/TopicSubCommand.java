package com.alibaba.rocketmq.tools.topic;

import com.alibaba.rocketmq.tools.SubCommand;


/**
 * @author shijia.wxr<vintage.wang@gmail.com>
 */
public class TopicSubCommand implements SubCommand {

    @Override
    public String commandName() {
        return "topic";
    }


    @Override
    public String commandDesc() {
        return "Update, create, delete or list topics";
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
