package com.alibaba.rocketmq.tools.command.topic;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import com.alibaba.rocketmq.tools.command.SubCommand;


/**
 * @author shijia.wxr<vintage.wang@gmail.com>
 */
public class TopicSubCommand implements SubCommand {

    @Override
    public String commandName() {
        return "topic";
    }


    @Override
    public String commandDesc() {
        return "Update, create, delete or list topics";
    }


    @Override
    public Options buildCommandlineOptions(final Options options) {
        Option opt = new Option("t", "topic", true, "topic name");
        opt.setRequired(false);
        options.addOption(opt);

        return options;
    }


    @Override
    public void execute(CommandLine commandLine) {

    }
}
