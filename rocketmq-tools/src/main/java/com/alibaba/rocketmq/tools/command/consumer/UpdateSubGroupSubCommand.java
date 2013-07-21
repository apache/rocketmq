package com.alibaba.rocketmq.tools.command.consumer;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import com.alibaba.rocketmq.common.subscription.SubscriptionGroupConfig;
import com.alibaba.rocketmq.tools.admin.DefaultMQAdminExt;
import com.alibaba.rocketmq.tools.command.SubCommand;


/**
 * –ﬁ∏ƒ°¢¥¥Ω®∂©‘ƒ◊È≈‰÷√√¸¡Ó
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-7-21
 */
public class UpdateSubGroupSubCommand implements SubCommand {

    @Override
    public String commandName() {
        return "updateSubGroup";
    }


    @Override
    public String commandDesc() {
        return "Update or create subscription group";
    }


    @Override
    public Options buildCommandlineOptions(Options options) {
        Option opt = new Option("b", "brokerAddr", true, "create subscription group to which broker");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("g", "groupName", true, "consumer group name");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("c", "consumeEnable", true, "consume enable");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("m", "consumeFromMinEnable", true, "from min offset");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("d", "consumeBroadcastEnable", true, "broadcast");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("q", "retryQueueNums", true, "retry queue nums");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("r", "retryMaxTimes", true, "retry max times");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("i", "brokerId", true, "consumer from which broker id");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("w", "whichBrokerWhenConsumeSlowly", true, "which broker id when consume slowly");
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
            SubscriptionGroupConfig subscriptionGroupConfig = new SubscriptionGroupConfig();
            subscriptionGroupConfig.setConsumeBroadcastEnable(false);
            subscriptionGroupConfig.setConsumeFromMinEnable(false);

            // brokerAddr
            if (commandLine.hasOption('b')) {
                addr = commandLine.getOptionValue('b');
            }
            else {
                System.out.println("please tell us broker's addr");
                return;
            }

            // groupName
            if (commandLine.hasOption('g')) {
                subscriptionGroupConfig.setGroupName(commandLine.getOptionValue('g'));
            }
            else {
                System.out.println("please tell us consumer group name");
                return;
            }

            // consumeEnable
            if (commandLine.hasOption('c')) {
                subscriptionGroupConfig
                    .setConsumeEnable(Boolean.parseBoolean(commandLine.getOptionValue('c')));
            }

            // consumeFromMinEnable
            if (commandLine.hasOption('m')) {
                subscriptionGroupConfig.setConsumeFromMinEnable(Boolean.parseBoolean(commandLine
                    .getOptionValue('m')));
            }

            // consumeBroadcastEnable
            if (commandLine.hasOption('d')) {
                subscriptionGroupConfig.setConsumeBroadcastEnable(Boolean.parseBoolean(commandLine
                    .getOptionValue('d')));
            }

            // retryQueueNums
            if (commandLine.hasOption('q')) {
                subscriptionGroupConfig.setRetryQueueNums(Integer.parseInt(commandLine.getOptionValue('q')));
            }

            // retryMaxTimes
            if (commandLine.hasOption('r')) {
                subscriptionGroupConfig.setRetryMaxTimes(Integer.parseInt(commandLine.getOptionValue('r')));
            }

            // brokerId
            if (commandLine.hasOption('i')) {
                subscriptionGroupConfig.setBrokerId(Long.parseLong(commandLine.getOptionValue('i')));
            }

            // whichBrokerWhenConsumeSlowly
            if (commandLine.hasOption('w')) {
                subscriptionGroupConfig.setWhichBrokerWhenConsumeSlowly(Long.parseLong(commandLine
                    .getOptionValue('w')));
            }

            defaultMQAdminExt.createAndUpdateSubscriptionGroupConfigByAddr(addr, subscriptionGroupConfig);
            System.out.println("create subscription group success.");
            System.out.println(subscriptionGroupConfig);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            defaultMQAdminExt.shutdown();
        }
    }
}
