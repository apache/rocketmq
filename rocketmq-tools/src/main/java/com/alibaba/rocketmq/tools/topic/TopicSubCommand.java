package com.alibaba.rocketmq.tools.topic;

import com.alibaba.rocketmq.tools.SubCommand;


/**
 * @author vintage.wang@gmail.com shijia.wxr@taobao.com
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
