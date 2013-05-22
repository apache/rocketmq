package com.alibaba.rocketmq.tools;

import java.util.ArrayList;
import java.util.List;

import com.alibaba.rocketmq.tools.topic.TopicManagerSubCommand;


/**
 * @author vintage.wang@gmail.com shijia.wxr@taobao.com
 */
public class MQAdminStartup {
    private static List<SubCommand> subCommandList = new ArrayList<SubCommand>();

    static {
        subCommandList.add(new TopicManagerSubCommand());
        subCommandList.add(new TopicManagerSubCommand());
        subCommandList.add(new TopicManagerSubCommand());
        subCommandList.add(new TopicManagerSubCommand());
        subCommandList.add(new TopicManagerSubCommand());
        subCommandList.add(new TopicManagerSubCommand());
    }


    private static void printHelp() {
        System.out.println("The most commonly used mqadmin commands are:");

        for (SubCommand cmd : subCommandList) {
            System.out.printf("   %-16s %s\n", cmd.commandName(), cmd.commandDesc());
        }

        System.out.println("\nSee 'mqadmin help <command>' for more information on a specific command.\n");
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
            printHelp();
            break;
        case 1:
            if (args[0].equals("help")) {
                printHelp();
            }
            else {

            }
        default:
            break;
        }
    }
}
