package com.alibaba.rocketmq.tools.command.offset;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;

import com.alibaba.rocketmq.common.MixAll;
import com.alibaba.rocketmq.remoting.RPCHook;
import com.alibaba.rocketmq.srvutil.ServerUtil;
import com.alibaba.rocketmq.tools.admin.DefaultMQAdminExt;
import com.alibaba.rocketmq.tools.command.SubCommand;


/**
 * 复制某一个 group 的 offset。
 * 
 * @author: manhong.yqd<jodie.yqd@gmail.com>
 * @since: 13-9-12
 */
public class CloneGroupOffsetCommand implements SubCommand {
    @Override
    public String commandName() {
        return "cloneGroupOffset";
    }


    @Override
    public String commandDesc() {
        return "clone offset from other group.";
    }


    @Override
    public Options buildCommandlineOptions(Options options) {
        Option opt = new Option("s", "srcGroup", true, "set source consumer group");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("d", "destGroup", true, "set destination consumer group");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("t", "topic", true, "set the topic");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("o", "offline", true, "the group or the topic is offline");
        opt.setRequired(false);
        options.addOption(opt);

        options.addOption(opt);

        return options;
    }


    @Override
    public void execute(CommandLine commandLine, Options options, RPCHook rpcHook) {
        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt(rpcHook);
        defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));
        try {
            String srcGroup = commandLine.getOptionValue("s").trim();
            String destGroup = commandLine.getOptionValue("d").trim();
            String topic = "";
            if (commandLine.hasOption('t')) {
                topic = commandLine.getOptionValue("t").trim();
            }

            boolean isOffline = false;
            if (commandLine.hasOption('o')) {
                isOffline = Boolean.parseBoolean(commandLine.getOptionValue("o").trim());
            }

            defaultMQAdminExt.start();
            defaultMQAdminExt.cloneGroupOffset(srcGroup, destGroup, topic, isOffline);
            System.out.printf("clone group offset success. srcGroup[%s], destGroup=[%s], topic[%s]",
                srcGroup, destGroup, topic);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            defaultMQAdminExt.shutdown();
        }
    }


    public static void main(String[] args) {
        System.setProperty(MixAll.NAMESRV_ADDR_PROPERTY, "127.0.0.1:9876");
        CloneGroupOffsetCommand cmd = new CloneGroupOffsetCommand();
        Options options = ServerUtil.buildCommandlineOptions(new Options());
        String[] subargs =
                new String[] { "-t qatest_TopicTest", "-g qatest_consumer", "-s 1389098416742", "-f true" };
        final CommandLine commandLine =
                ServerUtil.parseCmdLine("mqadmin " + cmd.commandName(), subargs,
                    cmd.buildCommandlineOptions(options), new PosixParser());
        cmd.execute(commandLine, options, null);
    }
}
