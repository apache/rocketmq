package com.alibaba.rocketmq.tools;

import java.util.ArrayList;
import java.util.List;

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


    public static void main(String[] args) {

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
                cmd.execute(parseSubArgs(args));
            }
            break;
        }
    }
}
