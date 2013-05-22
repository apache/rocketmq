/**
 * 
 */
package com.alibaba.rocketmq.tools;

/**
 * 
 * @author vintage.wang@gmail.com shijia.wxr@taobao.com
 */
public interface SubCommand {
    public String commandName();


    public String commandDesc();


    public void execute(String[] args);
}
