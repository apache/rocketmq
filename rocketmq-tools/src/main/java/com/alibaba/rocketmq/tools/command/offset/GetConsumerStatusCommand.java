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

        opt = new Option("i", "clientAddr", true, "set the client address(ip:port)");
        opt.setRequired(false);
        options.addOption(opt);

        return options;
    }


    @Override
    public void execute(CommandLine commandLine, Options options) {
        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt();
        defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));
        try {
            String group = commandLine.getOptionValue("g").trim();
            String topic = commandLine.getOptionValue("t").trim();
            String clientAddr = "";
            if (commandLine.hasOption("i")) {
                clientAddr = commandLine.getOptionValue("i").trim();
            }
            defaultMQAdminExt.start();

            Map<String, Map<MessageQueue, Long>> consumerStatusTable =
                    defaultMQAdminExt.getConsumeStatus(topic, group, clientAddr);
            System.out.printf("get consumer status from client. group=%s, topic=%s,clientAddr=%s\n", group,
                topic, clientAddr);

            System.out.printf("%-20s  %-80s  %-20s\n",//
                "#clientIp",//
                "#messageQueue",//
                "#offset");

            Iterator<String> clientIterator = consumerStatusTable.keySet().iterator();
            while (clientIterator.hasNext()) {
                String clientIp = clientIterator.next();
                Map<MessageQueue, Long> mqTable = consumerStatusTable.get(clientIp);
                Iterator<MessageQueue> mqIterator = mqTable.keySet().iterator();
                while (mqIterator.hasNext()) {
                    MessageQueue mq = mqIterator.next();
                    System.out.printf("%-20s  %-80s  %-20d\n",//
                        UtilAll.frontStringAtLeast(clientIp, 20),//
                        mq.toString(),//
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
        System.setProperty(MixAll.NAMESRV_ADDR_PROPERTY, "10.232.26.122:9876");
        GetConsumerStatusCommand cmd = new GetConsumerStatusCommand();
        Options options = MixAll.buildCommandlineOptions(new Options());
        String[] subargs = new String[] { "-t qatest_TopicTest", "-g qatest_consumer" };
        final CommandLine commandLine =
                MixAll.parseCmdLine("mqadmin " + cmd.commandName(), subargs,
		                cmd.buildCommandlineOptions(options), new PosixParser());
        cmd.execute(commandLine, options);
    }
}
