package org.apache.rocketmq.tools.command.namesrv;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.common.protocol.body.AclConfigData;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.SubCommand;
import org.apache.rocketmq.tools.command.SubCommandException;

public class AclConfigReadCommand implements SubCommand {
    @Override
    public String commandName() {

        return "aclRead";
    }

    @Override
    public String commandDesc() {
        return "ACL read function";
    }

    @Override
    public Options buildCommandlineOptions(Options options) {

        Option opt = new Option("i","instanceName", true, "instance name");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("t","topic", true, "topic name");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("r","read", true, "access read operation");
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
            // read operation
            String operation = commandLine.getOptionValue('r').trim();

            defaultMQAdminExt.start();

            AclConfigData aclConfigData = defaultMQAdminExt.aclReadConfig(instanceName, topic, operation);

            System.out.printf("%-50s  %-50s %-50s\n", aclConfigData.getInstanceName(),
                aclConfigData.getTopic(), aclConfigData.getOperation());
        } catch (Exception e){
            throw new SubCommandException(this.getClass().getSimpleName() + " command failed", e);
        } finally {
            defaultMQAdminExt.shutdown();
        }
    }
}
