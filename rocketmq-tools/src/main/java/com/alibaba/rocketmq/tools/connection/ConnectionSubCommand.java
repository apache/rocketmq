/**
 * 
 */
package com.alibaba.rocketmq.tools.connection;

import com.alibaba.rocketmq.tools.SubCommand;


/**
 * @author vintage.wang@gmail.com shijia.wxr@taobao.com
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
    public void printHelp() {
        // TODO Auto-generated method stub

    }


    @Override
    public void execute(String[] args) {
        // TODO Auto-generated method stub

    }

}
