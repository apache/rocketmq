package com.alibaba.rocketmq.tools.command.topic;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import com.alibaba.rocketmq.common.TopicConfig;
import com.alibaba.rocketmq.tools.admin.DefaultMQAdminExt;
import com.alibaba.rocketmq.tools.command.SubCommand;


/**
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-7-21
 */
public class UpdateTopicSubCommand implements SubCommand {

    @Override
    public String commandName() {
        return "updateTopic";
    }


    @Override
    public String commandDesc() {
        return "Update or create topic";
    }


    @Override
    public Options buildCommandlineOptions(Options options) {
        Option opt = new Option("t", "topic", true, "topic name");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("b", "brokerAddr", true, "create topic to which broker");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("r", "readQueueNums", true, "set read queue nums");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("w", "writeQueueNums", true, "set write queue nums");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("p", "perm", true, "set topic's permission(W|R|WR)");
        opt.setRequired(false);
        options.addOption(opt);

        return options;
    }


    @Override
    public void execute(CommandLine commandLine) {
        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt();
        try {
            defaultMQAdminExt.start();

            String addr = null;
            TopicConfig topicConfig = new TopicConfig();

            // brokerAddr
            if (commandLine.hasOption('b')) {
                addr = commandLine.getOptionValue('b');
            }
            else {
                System.out.println("please tell us broker's addr");
                return;
            }

            // topic
            if (commandLine.hasOption('t')) {
                topicConfig.setTopicName(commandLine.getOptionValue('t'));
            }
            else {
                System.out.println("please tell us topic name");
                return;
            }

            // readQueueNums
            if (commandLine.hasOption('r')) {
                topicConfig.setReadQueueNums(Integer.parseInt(commandLine.getOptionValue('r')));
            }

            // writeQueueNums
            if (commandLine.hasOption('w')) {
                topicConfig.setWriteQueueNums(Integer.parseInt(commandLine.getOptionValue('w')));
            }

            // perm
            if (commandLine.hasOption('p')) {
                topicConfig.setPerm(Integer.parseInt(commandLine.getOptionValue('p')));
            }

            defaultMQAdminExt.createAndUpdateTopicConfigByAddr(addr, topicConfig);
            System.out.println("create topic success.");
            System.out.println(topicConfig);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            defaultMQAdminExt.shutdown();
        }
    }
}
