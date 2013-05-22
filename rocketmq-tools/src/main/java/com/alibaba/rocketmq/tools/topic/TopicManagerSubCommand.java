package com.alibaba.rocketmq.tools.topic;

import com.alibaba.rocketmq.tools.SubCommand;


/**
 * @author vintage.wang@gmail.com shijia.wxr@taobao.com
 */
public class TopicManagerSubCommand implements SubCommand {

    @Override
    public String commandName() {
        return "topic";
    }


    @Override
    public String commandDesc() {
        return "Update, create, delete or list topics";
    }


    @Override
    public void execute(String[] args) {
        // TODO Auto-generated method stub

    }
}
