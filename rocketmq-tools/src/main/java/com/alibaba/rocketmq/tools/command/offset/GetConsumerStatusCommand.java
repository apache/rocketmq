package com.alibaba.rocketmq.tools.command.offset;

import java.util.Iterator;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;

import com.alibaba.rocketmq.common.MixAll;
import com.alibaba.rocketmq.common.UtilAll;
import com.alibaba.rocketmq.common.message.MessageQueue;
import com.alibaba.rocketmq.remoting.RPCHook;
import com.alibaba.rocketmq.srvutil.ServerUtil;
import com.alibaba.rocketmq.tools.admin.DefaultMQAdminExt;
import com.alibaba.rocketmq.tools.command.SubCommand;


/**
 * 根据时间来设置消费进度，设置之前要关闭这个订阅组的所有consumer，设置完再启动，方可生效。
 * 
 * @author: manhong.yqd<jodie.yqd@gmail.com>
 * @since: 13-9-12
 */
public class GetConsumerStatusCommand implements SubCommand {
    @Override
    public String commandName() {
        return "getConsumerStatus";
    }


    @Override
    public String commandDesc() {
        return "get consumer status from client.";
    }


    @Override
    public Options buildCommandlineOptions(Options options) {
        Option opt = new Option("g", "group", true, "set the consumer group");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("t", "topic", true, "set the topic");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("i", "originClientId", true, "set the consumer clientId");
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
            String originClientId = "";
            if (commandLine.hasOption("i")) {
                originClientId = commandLine.getOptionValue("i").trim();
            }
            defaultMQAdminExt.start();

            Map<String, Map<MessageQueue, Long>> consumerStatusTable =
                    defaultMQAdminExt.getConsumeStatus(topic, group, originClientId);
            System.out.printf("get consumer status from client. group=%s, topic=%s, originClientId=%s\n",
                group, topic, originClientId);

            System.out.printf("%-50s  %-15s  %-15s  %-20s\n",//
                "#clientId",//
                "#brokerName", //
                "#queueId",//
                "#offset");

            Iterator<String> clientIterator = consumerStatusTable.keySet().iterator();
            while (clientIterator.hasNext()) {
                String clientId = clientIterator.next();
                Map<MessageQueue, Long> mqTable = consumerStatusTable.get(clientId);
                Iterator<MessageQueue> mqIterator = mqTable.keySet().iterator();
                while (mqIterator.hasNext()) {
                    MessageQueue mq = mqIterator.next();
                    System.out.printf("%-50s  %-15s  %-15d  %-20d\n",//
                        UtilAll.frontStringAtLeast(clientId, 50),//
                        mq.getBrokerName(),//
                        mq.getQueueId(),//
                        mqTable.get(mq));
                }
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
        GetConsumerStatusCommand cmd = new GetConsumerStatusCommand();
        Options options = ServerUtil.buildCommandlineOptions(new Options());
        String[] subargs = new String[] { "-t qatest_TopicTest", "-g qatest_consumer_broadcast" };
        final CommandLine commandLine =
                ServerUtil.parseCmdLine("mqadmin " + cmd.commandName(), subargs,
                    cmd.buildCommandlineOptions(options), new PosixParser());
        cmd.execute(commandLine, options, null);
    }
}
