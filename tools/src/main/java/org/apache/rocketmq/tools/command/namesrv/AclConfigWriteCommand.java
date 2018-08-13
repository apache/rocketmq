package org.apache.rocketmq.tools.command.namesrv;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.SubCommand;
import org.apache.rocketmq.tools.command.SubCommandException;

public class AclConfigWriteCommand implements SubCommand {
    @Override
    public String commandName() {
        return "aclWrite";
    }

    @Override
    public String commandDesc() {
        return "ACL write function.";
    }

    @Override
    public Options buildCommandlineOptions(Options options) {
        Option opt = new Option("i","instanceName", true, "instance name");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("t","topic", true, "topic name");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("o","operation", true, "access control operation");
        opt.setRequired(true);
        options.addOption(opt);

        return options;
    }

    @Override
    public void execute(CommandLine commandLine, Options options, RPCHook rpcHook)
        throws SubCommandException {
        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt(rpcHook);
        defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));
        try {
            // instance name
            String instanceName = commandLine.getOptionValue('i').trim();
            // topic name
            String topic = commandLine.getOptionValue('t').trim();
            // write operation
            String operation = commandLine.getOptionValue('o').trim();

            defaultMQAdminExt.start();
            defaultMQAdminExt.aclWriteConfig(instanceName, topic, operation);
            System.out.printf("acl write operate success.%n");
        } catch (Exception e){
            throw new SubCommandException(this.getClass().getSimpleName() + " command failed", e);
        } finally {
            defaultMQAdminExt.shutdown();
        }

    }
}
