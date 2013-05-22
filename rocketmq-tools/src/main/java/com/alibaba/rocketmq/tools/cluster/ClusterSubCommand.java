package com.alibaba.rocketmq.tools.cluster;

import com.alibaba.rocketmq.tools.SubCommand;


/**
 * @author vintage.wang@gmail.com shijia.wxr@taobao.com
 */
public class ClusterSubCommand implements SubCommand {

    @Override
    public String commandName() {
        return "cluster";
    }


    @Override
    public String commandDesc() {
        return "List all of clusters or someone";
    }


    @Override
    public void printHelp() {
    }


    @Override
    public void execute(String[] args) {
    }
}
