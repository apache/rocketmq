package org.apache.rocketmq.tools.command.stats;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.common.TracerTime;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.CommandUtil;
import org.apache.rocketmq.tools.command.SubCommand;
import org.apache.rocketmq.tools.command.SubCommandException;

public class QueryTracerTimeCommand implements SubCommand {
    @Override
    public String commandName() {
        return "queryTracerTimeCommand";
    }

    @Override
    public String commandDesc() {
        return "queryTracerTimeCommand";
    }

    @Override
    public Options buildCommandlineOptions(Options options) {
        Option opt = new Option("id", "tracerTimeId", true, "tracerTimeId");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("c", "clusterName", true, "clusterName");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("b", "broker", true, "broker addr.");
        opt.setRequired(false);
        options.addOption(opt);

        return options;
    }

    @Override
    public void execute(CommandLine commandLine, Options options,
        RPCHook rpcHook) throws SubCommandException {
        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt(rpcHook);
        defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));
        try {
            Set<String> masterSet = new HashSet<>();

            String clusterName = null;
            String tracerTimeId = commandLine.getOptionValue("id").trim();
            String brokerAddr = "";
            String topic = "";

            if (commandLine.hasOption("b")) {
                brokerAddr = commandLine.getOptionValue("b").trim();
                masterSet.add(brokerAddr);
            }


            if (commandLine.hasOption("t")) {
                TopicRouteData topicRouteData = defaultMQAdminExt.examineTopicRouteInfo(topic);
                if (topicRouteData != null && topicRouteData.getBrokerDatas() != null) {
                    List<BrokerData> brokerDataList = topicRouteData.getBrokerDatas();
                    for (BrokerData brokerData : brokerDataList) {
                        masterSet.add(brokerData.selectBrokerAddr());
                    }
                }
            }

            if (commandLine.hasOption("c")) {
                clusterName = commandLine.getOptionValue("c").trim();
            }

            defaultMQAdminExt.start();
            if (masterSet.isEmpty()) {
                masterSet = CommandUtil.fetchMasterAddrByClusterName(defaultMQAdminExt,
                    clusterName);
            }

            String formatter = "%n%-35s  %-18s  %-23s  %-23s %-23s %-23s %-23s %-23s %n";

            System.out.printf(formatter,
                "#tracerTimeId",
                "#brokerAddr",
                "#messageCreateTime",
                "#messageSendTime",
                "#messageArriveBrokerTime",
                "#messageBeginSaveTime",
                "#messageSaveEndTime",
                "#brokerSendAckTime");

            SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
            for (String broker : masterSet) {
                TracerTime tracerTime = defaultMQAdminExt.queryTracerTime(broker, null, tracerTimeId);
                if (tracerTime != null) {
                    System.out.printf(formatter,
                        tracerTimeId,
                        broker,
                        df.format(new Date(tracerTime.getMessageCreateTime())),
                        df.format(new Date(tracerTime.getMessageSendTime())),
                        df.format(new Date(tracerTime.getMessageArriveBrokerTime())),
                        df.format(new Date(tracerTime.getMessageBeginSaveTime())),
                        df.format(new Date(tracerTime.getMessageSaveEndTime())),
                        df.format(new Date(tracerTime.getBrokerSendAckTime())));
                }
            }

        } catch (Exception e) {
            throw new SubCommandException(this.getClass().getSimpleName() + " command failed", e);
        } finally {
            defaultMQAdminExt.shutdown();
        }
    }
}
