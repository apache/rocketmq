package com.alibaba.rocketmq.tools.command.namesrv;

import com.alibaba.rocketmq.tools.admin.DefaultMQAdminExt;
import com.alibaba.rocketmq.tools.command.SubCommand;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;


/**
 * description for com.alibaba.rocketmq.tools.command.namesrv. User: manhong.yqd
 */
public class DeleteKvConfig implements SubCommand {
    @Override
    public String commandName() {
        return "deleteKvConfig";
    }


    @Override
    public String commandDesc() {
        return "delete KV config.";
    }


    @Override
    public Options buildCommandlineOptions(Options options) {
        Option opt = new Option("s", "namespace", true, "set the namespace");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("k", "key", true, "set the key name");
        opt.setRequired(true);
        options.addOption(opt);
        return options;
    }


    @Override
    public void execute(CommandLine commandLine, Options options) {
        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt();
        defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));
        try {
            // namespace
            String namespace = commandLine.getOptionValue('s');
            // key name
            String key = commandLine.getOptionValue('k');

            defaultMQAdminExt.start();
            defaultMQAdminExt.deleteKvConfig(namespace, key);
            System.out.printf("delete kv config from namespace success.\n");
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
