package com.alibaba.rocketmq.tools;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.core.joran.spi.JoranException;

import com.alibaba.rocketmq.common.MixAll;
import com.alibaba.rocketmq.tools.broker.BrokerSubCommand;
import com.alibaba.rocketmq.tools.cluster.ClusterSubCommand;
import com.alibaba.rocketmq.tools.connection.ConnectionSubCommand;
import com.alibaba.rocketmq.tools.consumer.ConsumerSubCommand;
import com.alibaba.rocketmq.tools.message.MessageSubCommand;
import com.alibaba.rocketmq.tools.namesrv.NamesrvSubCommand;
import com.alibaba.rocketmq.tools.producer.ProducerSubCommand;
import com.alibaba.rocketmq.tools.stats.StatsSubCommand;
import com.alibaba.rocketmq.tools.topic.TopicSubCommand;


/**
 * @author shijia.wxr<vintage.wang@gmail.com>
 */
public class MQAdminStartup {
    private static List<SubCommand> subCommandList = new ArrayList<SubCommand>();

    static {
        subCommandList.add(new TopicSubCommand());
        subCommandList.add(new ClusterSubCommand());
        subCommandList.add(new BrokerSubCommand());
        subCommandList.add(new NamesrvSubCommand());
        subCommandList.add(new ProducerSubCommand());
        subCommandList.add(new ConsumerSubCommand());
        subCommandList.add(new ConnectionSubCommand());
        subCommandList.add(new MessageSubCommand());
        subCommandList.add(new StatsSubCommand());
    }


    private static void printHelp() {
        System.out.println("The most commonly used mqadmin commands are:");

        for (SubCommand cmd : subCommandList) {
            System.out.printf("   %-11s %s\n", cmd.commandName(), cmd.commandDesc());
        }

        System.out.println("\nSee 'mqadmin help <command>' for more information on a specific command.");
    }


    private static SubCommand findSubCommand(final String name) {
        for (SubCommand cmd : subCommandList) {
            if (cmd.commandName().equals(name)) {
                return cmd;
            }
        }

        return null;
    }


    private static String[] parseSubArgs(String[] args) {
        if (args.length > 1) {
            String[] result = new String[args.length - 1];
            for (int i = 0; i < args.length - 1; i++) {
                result[i] = args[i + 1];
            }
            return result;
        }
        return null;
    }


    private static void initLogback() throws JoranException {
        String rocketmqHome =
                System.getProperty(MixAll.ROCKETMQ_HOME_PROPERTY, System.getenv(MixAll.ROCKETMQ_HOME_ENV));

        // ³õÊ¼»¯Logback
        LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();
        JoranConfigurator configurator = new JoranConfigurator();
        configurator.setContext(lc);
        lc.reset();
        configurator.doConfigure(rocketmqHome + "/conf/log4j_tools.xml");
    }


    public static void main(String[] args) {
        try {
            switch (args.length) {
            case 0:
            case 1:
                printHelp();
                break;
            case 2:
                if (args[0].equals("help")) {
                    SubCommand cmd = findSubCommand(args[1]);
                    if (cmd != null) {
                        cmd.printHelp();
                    }
                    break;
                }
            default:
                SubCommand cmd = findSubCommand(args[0]);
                if (cmd != null) {
                    initLogback();
                    cmd.execute(parseSubArgs(args));
                }
                break;
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}
