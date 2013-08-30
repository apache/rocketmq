package com.alibaba.rocketmq.tools.command.namesrv;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import com.alibaba.rocketmq.common.namesrv.NamesrvUtil;
import com.alibaba.rocketmq.tools.admin.DefaultMQAdminExt;
import com.alibaba.rocketmq.tools.command.SubCommand;


/**
 * 添加或者更新 project group 配置信息
 * 
 * @author: manhong.yqd<jodie.yqd@gmail.com>
 * @since: 13-8-29
 */
public class UpdateProjectGroupCommand implements SubCommand {
    @Override
    public String commandName() {
        return "updateProjectGroup";
    }


    @Override
    public String commandDesc() {
        return "create or update project group by server ip.";
    }


    @Override
    public Options buildCommandlineOptions(Options options) {
        Option opt = new Option("i", "ip", true, "set the server ip");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("p", "project", true, "set the project group");
        opt.setRequired(true);
        options.addOption(opt);
        return options;
    }


    @Override
    public void execute(CommandLine commandLine, Options options) {
        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt();
        defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));
        try {
            String namespace = NamesrvUtil.NAMESPACE_PROJECT_CONFIG;
            String ip = commandLine.getOptionValue('i');
            String project = commandLine.getOptionValue('p');

            defaultMQAdminExt.start();
            defaultMQAdminExt.createAndUpdateKvConfig(namespace, ip, project);
            System.out.printf("create or update kv config to namespace success.\n");
            return;
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            defaultMQAdminExt.shutdown();
        }
    }
}
