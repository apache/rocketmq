package com.alibaba.rocketmq.tools.command.consumer;

import java.util.Set;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import com.alibaba.rocketmq.common.MixAll;
import com.alibaba.rocketmq.tools.admin.DefaultMQAdminExt;
import com.alibaba.rocketmq.tools.command.CommandUtil;
import com.alibaba.rocketmq.tools.command.SubCommand;


/**
 * 删除订阅组配置命令
 * 
 * @author manhong.yqd<manhong.yqd@alibaba-inc.com>
 * @since 2013-8-22
 */
public class DeleteSubscriptionGroupCommand implements SubCommand {
    @Override
    public String commandName() {
        return "deleteSubGroup";
    }


    @Override
    public String commandDesc() {
        return "delete subscription group from broker.";
    }


    @Override
    public Options buildCommandlineOptions(Options options) {
        Option opt = new Option("b", "brokerAddr", true, "delete subscription group from which broker");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("c", "clusterName", true, "delete subscription group from which cluster");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("g", "groupName", true, "subscription group name");
        opt.setRequired(true);
        options.addOption(opt);

        return options;
    }


    @Override
    public void execute(CommandLine commandLine, Options options) {
        DefaultMQAdminExt adminExt = new DefaultMQAdminExt();
        adminExt.setInstanceName(Long.toString(System.currentTimeMillis()));
        try {
            // groupName
            String groupName = commandLine.getOptionValue('g');

            if (commandLine.hasOption('b')) {
                String addr = commandLine.getOptionValue('b');
                adminExt.start();

                adminExt.deleteSubscriptionGroup(addr, groupName);
                System.out.printf("delete subscription group [%s] from broker [%s] success.\n", groupName,
                    addr);


                return;
            }
            else if (commandLine.hasOption('c')) {
                String clusterName = commandLine.getOptionValue('c');
                adminExt.start();

                Set<String> masterSet = CommandUtil.fetchMasterAddrByClusterName(adminExt, clusterName);
                for (String master : masterSet) {
                    adminExt.deleteSubscriptionGroup(master, groupName);
                    System.out.printf(
                        "delete subscription group [%s] from broker [%s] in cluster [%s] success.\n",
                        groupName, master, clusterName);
                }

                return;
            }

            MixAll.printCommandLineHelp("mqadmin " + this.commandName(), options);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            adminExt.shutdown();
        }
    }
}
