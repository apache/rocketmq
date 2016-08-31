package com.alibaba.rocketmq.tools.command.message;

import com.alibaba.rocketmq.common.UtilAll;
import com.alibaba.rocketmq.common.message.MessageClientIDSetter;
import com.alibaba.rocketmq.remoting.RPCHook;
import com.alibaba.rocketmq.tools.command.SubCommand;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

/**
 * Created by jodie on 16/8/18.
 */
public class DecodeMessageIdCommond implements SubCommand {
    @Override
    public String commandName() {
        return "DecodeMessageId";
    }


    @Override
    public String commandDesc() {
        return "decode unique message ID";
    }


    @Override
    public Options buildCommandlineOptions(Options options) {
        Option opt = new Option("i", "messageId", true, "unique message ID");
        opt.setRequired(false);
        options.addOption(opt);
        return options;
    }


    @Override
    public void execute(final CommandLine commandLine, final Options options, RPCHook rpcHook) {
        String messageId = commandLine.getOptionValue('i').trim();

        try {
            System.out.println("ip=" + MessageClientIDSetter.getIPStrFromID(messageId));
        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            String date = UtilAll.formatDate(MessageClientIDSetter.getNearlyTimeFromID(messageId), UtilAll.yyyy_MM_dd_HH_mm_ss_SSS);
            System.out.println("date=" + date);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        System.out.println(MessageClientIDSetter.getIPStrFromID("783786393794180EEDC3636C5D3A0011"));
        System.out.println(MessageClientIDSetter.getIPStrFromID("783786393794180EEDC3636C5D420013"));
        System.out.println(MessageClientIDSetter.getIPStrFromID("783786393794180EEDC36386D07B001F"));
    }
}
