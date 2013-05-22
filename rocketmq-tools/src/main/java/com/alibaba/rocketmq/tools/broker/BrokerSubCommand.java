/**
 * 
 */
package com.alibaba.rocketmq.tools.broker;

import com.alibaba.rocketmq.tools.SubCommand;


/**
 * @author vintage.wang@gmail.com shijia.wxr@taobao.com
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
    public void printHelp() {
        // TODO Auto-generated method stub

    }


    @Override
    public void execute(String[] args) {
        // TODO Auto-generated method stub

    }

}
