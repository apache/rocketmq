package com.alibaba.rocketmq.tools.command.namesrv;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;

import com.alibaba.rocketmq.common.MixAll;
import com.alibaba.rocketmq.common.UtilALl;
import com.alibaba.rocketmq.tools.admin.DefaultMQAdminExt;
import com.alibaba.rocketmq.tools.command.SubCommand;


/**
 * 取得 project group 配置信息
 * 
 * @author: manhong.yqd<jodie.yqd@gmail.com>
 * @since: 13-8-29
 */
public class GetProjectGroupCommand implements SubCommand {
    @Override
    public String commandName() {
        return "getProjectGroup";
    }


    @Override
    public String commandDesc() {
        return "get project group by server ip or project group name.";
    }


    @Override
    public Options buildCommandlineOptions(Options options) {
        Option opt = new Option("i", "ip", true, "set the server ip");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("p", "project", true, "set the project group");
        opt.setRequired(false);
        options.addOption(opt);
        return options;
    }


    @Override
    public void execute(CommandLine commandLine, Options options) {
        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt();
        defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));
        try {
            if (commandLine.hasOption("i")) {
                String ip = commandLine.getOptionValue('i').trim();
                defaultMQAdminExt.start();
                String project = defaultMQAdminExt.getProjectGroupByIp(ip);
                System.out.printf("ip=%s, projectGroup=%s\n", ip, project);
            }
            else if (commandLine.hasOption("p")) {
                String project = commandLine.getOptionValue('p').trim();
                defaultMQAdminExt.start();
                String ips = defaultMQAdminExt.getIpsByProjectGroup(project);
                if (UtilALl.isBlank(ips)) {
                    System.out.printf("No ip in project group[%s]\n", project);
                } else {
	                System.out.printf("projectGroup=%s, ips=%s\n", project, ips);
                }
            }
            else {
                MixAll.printCommandLineHelp("mqadmin " + this.commandName(), options);
            }
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
        GetProjectGroupCommand cmd = new GetProjectGroupCommand();
        Options options = MixAll.buildCommandlineOptions(new Options());
        String[] subargs = new String[] { "-i 10.14.24.165","-p devgrouptest" };
        final CommandLine commandLine =
                MixAll.parseCmdLine("mqadmin " + cmd.commandName(), subargs,
                    cmd.buildCommandlineOptions(options), new PosixParser());
        cmd.execute(commandLine, options);
    }
}
