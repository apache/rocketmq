package com.alibaba.rocketmq.tools.command.broker;

import com.alibaba.rocketmq.remoting.RPCHook;
import com.alibaba.rocketmq.tools.admin.DefaultMQAdminExt;
import com.alibaba.rocketmq.tools.command.SubCommand;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;


/**
 * @auther manhong.yqd<jodie.yqd@gmail.com>
 */
public class CleanUnusedTopicCommand implements SubCommand {

    @Override
    public String commandName() {
        return "cleanUnusedTopic";
    }


    @Override
    public String commandDesc() {
        return "Clean unused topic on broker.";
    }


    @Override
    public Options buildCommandlineOptions(Options options) {
        Option opt = new Option("b", "brokerAddr", true, "Broker address");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("c", "cluster", true, "cluster name");
        opt.setRequired(false);
        options.addOption(opt);

        return options;
    }


    @Override
    public void execute(CommandLine commandLine, Options options, RPCHook rpcHook) {
        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt(rpcHook);
        defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));

        try {
            boolean result = false;
            defaultMQAdminExt.start();
            if (commandLine.hasOption('b')) {
                String addr = commandLine.getOptionValue('b').trim();
                result = defaultMQAdminExt.cleanUnusedTopicByAddr(addr);

            } else {
                String cluster = commandLine.getOptionValue('c');
                if (null != cluster)
                    cluster = cluster.trim();
                result = defaultMQAdminExt.cleanUnusedTopicByAddr(cluster);
            }
            System.out.println(result ? "success" : "false");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            defaultMQAdminExt.shutdown();
        }
    }
}
