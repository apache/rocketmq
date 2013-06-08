/**
 * 
 */
package com.alibaba.rocketmq.tools;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;


/**
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 */
public interface SubCommand {
    public String commandName();


    public String commandDesc();


    public Options buildCommandlineOptions(final Options options);


    public void execute(CommandLine commandLine);
}
