package com.alibaba.rocketmq.tools.command.message;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import com.alibaba.rocketmq.remoting.RPCHook;
import com.alibaba.rocketmq.tools.command.SubCommand;


/**
 * @auther lansheng.zj
 */
public class CheckMsgSubCommand implements SubCommand {
    @Override
    public String commandName() {
        return "checkMsg";
    }


    @Override
    public String commandDesc() {
        return "Check Message Store";
    }


    @Override
    public Options buildCommandlineOptions(Options options) {
        Option opt = new Option("p", "cStorePath", true, "cStorePath");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("s", "cSize ", true, "cSize");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("l", "lStorePath ", true, "lStorePath");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("z", "lSize ", true, "lSize");
        opt.setRequired(true);
        options.addOption(opt);

        return options;
    }


    @Override
    public void execute(CommandLine commandLine, Options options, RPCHook rpcHook) {
        Store store = new Store(commandLine.getOptionValue("cStorePath").trim(), //
            Integer.parseInt(commandLine.getOptionValue("cSize").trim()),//
            commandLine.getOptionValue("lStorePath").trim(), //
            Integer.parseInt(commandLine.getOptionValue("lSize").trim()));
        store.load();
        store.traval(false);
    }
}
