/**
 * 
 */
package com.alibaba.rocketmq.tools;

/**
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 */
public interface SubCommand {
    public String commandName();


    public String commandDesc();


    public void printHelp();


    public void execute(String[] args);
}
