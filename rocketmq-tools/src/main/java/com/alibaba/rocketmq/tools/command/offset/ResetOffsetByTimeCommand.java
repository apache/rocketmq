package com.alibaba.rocketmq.tools.command.offset;

import java.util.Iterator;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;

import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.MixAll;
import com.alibaba.rocketmq.common.UtilAll;
import com.alibaba.rocketmq.common.message.MessageQueue;
import com.alibaba.rocketmq.common.protocol.ResponseCode;
import com.alibaba.rocketmq.remoting.RPCHook;
import com.alibaba.rocketmq.srvutil.ServerUtil;
import com.alibaba.rocketmq.tools.admin.DefaultMQAdminExt;
import com.alibaba.rocketmq.tools.command.SubCommand;


/**
 * 根据时间设置消费进度，客户端无需重启。
 * 
 * @author: manhong.yqd<jodie.yqd@gmail.com>
 * @since: 13-9-12
 */
public class ResetOffsetByTimeCommand implements SubCommand {
    @Override
    public String commandName() {
        return "resetOffsetByTime";
    }


    @Override
    public String commandDesc() {
        return "Reset consumer offset by timestamp(without client restart).";
    }


    @Override
    public Options buildCommandlineOptions(Options options) {
        Option opt = new Option("g", "group", true, "set the consumer group");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("t", "topic", true, "set the topic");
        opt.setRequired(true);
        options.addOption(opt);

        opt =
                new Option("s", "timestamp", true,
                    "set the timestamp[currentTimeMillis|yyyy-MM-dd#HH:mm:ss:SSS]");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("f", "force", true, "set the force rollback by timestamp switch[true|false]");
        opt.setRequired(false);
        options.addOption(opt);
        return options;
    }


    @Override
    public void execute(CommandLine commandLine, Options options, RPCHook rpcHook) {
        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt(rpcHook);
        defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));
        try {
            String group = commandLine.getOptionValue("g").trim();
            String topic = commandLine.getOptionValue("t").trim();
            String timeStampStr = commandLine.getOptionValue("s").trim();
            long timestamp = 0;
            try {
                // 直接输入 long 类型的 timestamp
                timestamp = Long.valueOf(timeStampStr);
            }
            catch (NumberFormatException e) {
                // 输入的为日期格式，精确到毫秒
                timestamp = UtilAll.parseDate(timeStampStr, UtilAll.yyyy_MM_dd_HH_mm_ss_SSS).getTime();
            }

            boolean force = true;
            if (commandLine.hasOption('f')) {
                force = Boolean.valueOf(commandLine.getOptionValue("f").trim());
            }

            defaultMQAdminExt.start();
            Map<MessageQueue, Long> offsetTable;
            try {
                offsetTable = defaultMQAdminExt.resetOffsetByTimestamp(topic, group, timestamp, force);
            }
            catch (MQClientException e) {
                if (ResponseCode.CONSUMER_NOT_ONLINE == e.getResponseCode()) {
                    ResetOffsetByTimeOldCommand.resetOffset(defaultMQAdminExt, group, topic, timestamp,
                        force, timeStampStr);
                    return;
                }
                throw e;
            }

            System.out
                .printf(
                    "rollback consumer offset by specified group[%s], topic[%s], force[%s], timestamp(string)[%s], timestamp(long)[%s]\n",
                    group, topic, force, timeStampStr, timestamp);

            System.out.printf("%-40s  %-40s  %-40s\n",//
                "#brokerName",//
                "#queueId",//
                "#offset");

            Iterator<Map.Entry<MessageQueue, Long>> iterator = offsetTable.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<MessageQueue, Long> entry = iterator.next();
                System.out.printf("%-40s  %-40d  %-40d\n",//
                    UtilAll.frontStringAtLeast(entry.getKey().getBrokerName(), 32),//
                    entry.getKey().getQueueId(),//
                    entry.getValue());
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
        ResetOffsetByTimeCommand cmd = new ResetOffsetByTimeCommand();
        Options options = ServerUtil.buildCommandlineOptions(new Options());
        String[] subargs =
                new String[] { "-t qatest_TopicTest", "-g qatest_consumer", "-s 1389098416742", "-f true" };
        final CommandLine commandLine =
                ServerUtil.parseCmdLine("mqadmin " + cmd.commandName(), subargs,
                    cmd.buildCommandlineOptions(options), new PosixParser());
        cmd.execute(commandLine, options, null);
    }
}
