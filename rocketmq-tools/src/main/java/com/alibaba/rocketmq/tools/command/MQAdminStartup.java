package com.alibaba.rocketmq.tools.command;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.core.joran.spi.JoranException;

import com.alibaba.rocketmq.common.MQVersion;
import com.alibaba.rocketmq.common.MixAll;
import com.alibaba.rocketmq.remoting.protocol.RemotingCommand;
import com.alibaba.rocketmq.tools.command.consumer.UpdateSubGroupSubCommand;
import com.alibaba.rocketmq.tools.command.topic.UpdateTopicSubCommand;


/**
 * @author shijia.wxr<vintage.wang@gmail.com>
 */
public class MQAdminStartup {
    private static List<SubCommand> subCommandList = new ArrayList<SubCommand>();

    static {
        subCommandList.add(new UpdateTopicSubCommand());
        subCommandList.add(new UpdateSubGroupSubCommand());
        // subCommandList.add(new ClusterSubCommand());
        // subCommandList.add(new BrokerSubCommand());
        // subCommandList.add(new NamesrvSubCommand());
        // subCommandList.add(new ProducerSubCommand());
        // subCommandList.add(new ConsumerSubCommand());
        // subCommandList.add(new ConnectionSubCommand());
        // subCommandList.add(new MessageSubCommand());
        // subCommandList.add(new StatsSubCommand());
    }


    private static void printHelp() {
        System.out.println("The most commonly used mqadmin commands are:");

        for (SubCommand cmd : subCommandList) {
            System.out.printf("   %-16s %s\n", cmd.commandName(), cmd.commandDesc());
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

        // 初始化Logback
        LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();
        JoranConfigurator configurator = new JoranConfigurator();
        configurator.setContext(lc);
        lc.reset();
        configurator.doConfigure(rocketmqHome + "/conf/logback_tools.xml");
    }


    public static void main(String[] args) {
        // 设置当前程序版本号，每次发布版本时，都要修改CurrentVersion
        System.setProperty(RemotingCommand.RemotingVersionKey, Integer.toString(MQVersion.CurrentVersion));

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
                        Options options = MixAll.buildCommandlineOptions(new Options());
                        options = cmd.buildCommandlineOptions(options);
                        if (options != null) {
                            MixAll.printCommandLineHelp("mqadmin " + cmd.commandName(), options);
                        }
                    }
                    break;
                }
            default:
                SubCommand cmd = findSubCommand(args[0]);
                if (cmd != null) {
                    // 初始化日志
                    initLogback();

                    // 将main中的args转化为子命令的args（去除第一个参数）
                    String[] subargs = parseSubArgs(args);

                    // 解析命令行
                    Options options = MixAll.buildCommandlineOptions(new Options());
                    final CommandLine commandLine =
                            MixAll.parseCmdLine("mqadmin " + cmd.commandName(), subargs,
                                cmd.buildCommandlineOptions(options), new PosixParser());
                    if (null == commandLine) {
                        System.exit(-1);
                        return;
                    }

                    cmd.execute(commandLine);
                }
                break;
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}
