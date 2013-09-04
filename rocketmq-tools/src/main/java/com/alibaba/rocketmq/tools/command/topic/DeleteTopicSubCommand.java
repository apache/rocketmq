package com.alibaba.rocketmq.tools.command.topic;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;

import com.alibaba.rocketmq.common.MixAll;
import com.alibaba.rocketmq.tools.admin.DefaultMQAdminExt;
import com.alibaba.rocketmq.tools.command.CommandUtil;
import com.alibaba.rocketmq.tools.command.SubCommand;


/**
 * 删除Topic配置命令
 * 
 * @author manhong.yqd<manhong.yqd@alibaba-inc.com>
 * @since 2013-8-21
 */
public class DeleteTopicSubCommand implements SubCommand {
    @Override
    public String commandName() {
        return "deleteTopic";
    }


    @Override
    public String commandDesc() {
        return "delete topic from broker and NameServer.";
    }


    @Override
    public Options buildCommandlineOptions(Options options) {
        Option opt = new Option("t", "topic", true, "topic name");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("c", "clusterName", true, "delete topic from which cluster");
        opt.setRequired(true);
        options.addOption(opt);

        return options;
    }


    @Override
    public void execute(CommandLine commandLine, Options options) {
        DefaultMQAdminExt adminExt = new DefaultMQAdminExt();
        adminExt.setInstanceName(Long.toString(System.currentTimeMillis()));
        try {
            String topic = commandLine.getOptionValue('t').trim();

            if (commandLine.hasOption('c')) {
                String clusterName = commandLine.getOptionValue('c').trim();

                adminExt.start();
                // 删除 broker 上的 topic 信息
                Set<String> masterSet = CommandUtil.fetchMasterAddrByClusterName(adminExt, clusterName);
                adminExt.deleteTopicInBroker(masterSet, topic);
                System.out.printf("delete topic [%s] from cluster [%s] success.\n", topic, clusterName);

                // 删除 NameServer 上的 topic 信息
                Set<String> nameServerSet = null;
                if (commandLine.hasOption('n')) {
                    String[] ns = commandLine.getOptionValue('n').trim().split(";");
                    nameServerSet = new HashSet(Arrays.asList(ns));
                }
                adminExt.deleteTopicInNameServer(nameServerSet, topic);
                System.out.printf("delete topic [%s] from NameServer success.\n", topic);
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


    public static void main(String[] args) {
	    System.setProperty(MixAll.NAMESRV_ADDR_PROPERTY, "10.232.26.122:9876");
	    DeleteTopicSubCommand cmd = new DeleteTopicSubCommand();
	    Options options = MixAll.buildCommandlineOptions(new Options());
        String[] subargs = new String[] { "-t SELF_TEST_TOPIC", "-c jodie" };
        final CommandLine commandLine =
                MixAll.parseCmdLine("mqadmin " + cmd.commandName(), subargs,
                    cmd.buildCommandlineOptions(options), new PosixParser());
        cmd.execute(commandLine, options);
    }
}
