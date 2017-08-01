/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.namesrv.telnet;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.StringTokenizer;
import java.util.Map.Entry;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.admin.ConsumeStats;
import org.apache.rocketmq.common.admin.OffsetWrapper;
import org.apache.rocketmq.common.admin.TopicOffset;
import org.apache.rocketmq.common.admin.TopicStatsTable;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.body.Connection;
import org.apache.rocketmq.common.protocol.body.ConsumerConnection;
import org.apache.rocketmq.common.protocol.body.ProducerConnection;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.SubCommand;
import org.apache.rocketmq.tools.command.connection.ConsumerConnectionSubCommand;
import org.apache.rocketmq.tools.command.connection.ProducerConnectionSubCommand;
import org.apache.rocketmq.tools.command.consumer.ConsumerProgressSubCommand;
import org.apache.rocketmq.tools.command.topic.TopicStatusSubCommand;

public class TelnetCommandUtil {

    private static final String ERROR = " cmd error \r\n";
    private static final String EXE_ERROR = " execute error ";
    private static final String DATA_NULL = " execute get data null \r\n";
    private static String namesrvAddr = "";
    private static final String P_CON = "producerConnection";
    private static final String T_STATUS = "topicStatus";
    private static final String C_CON = "consumerConnection";
    private static final String C_PRO = "consumerProgress";
    static List<SubCommand> subCommandList = new ArrayList<SubCommand>();

    static {
        subCommandList.add(new ProducerConnectionSubCommand());
        subCommandList.add(new TopicStatusSubCommand());
        subCommandList.add(new ConsumerConnectionSubCommand());
        subCommandList.add(new ConsumerProgressSubCommand());

    }

    public static void setNamseSrvAddr(String nameSrvAddr) {
        namesrvAddr = nameSrvAddr;
        System.setProperty(MixAll.NAMESRV_ADDR_PROPERTY, namesrvAddr);
    }

    /**
     * @param str
     * @return
     */
    public static String doCmd(String str) {
        CommandLineParser parser = new PosixParser();
        StringTokenizer token = new StringTokenizer(str);
        String[] data = new String[token.countTokens()];
        int i = 0;
        while (token.hasMoreTokens()) {
            String tmp = token.nextToken();
            data[i] = tmp;
            i++;
        }
        SubCommand cmd = findSubCommand(data[0]);
        if (cmd == null)
            return ERROR;
        String[] subargs = parseSubArgs(data);

        Options options = ServerUtil.buildCommandlineOptions(new Options());
        cmd.buildCommandlineOptions(options);
        CommandLine commandLine = null;
        try {
            commandLine = parser.parse(options, subargs);
        } catch (ParseException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            return ERROR;
        }

        //cmd.execute(commandLine, options, null);
        String response = null;
        if (cmd.commandName().equalsIgnoreCase(T_STATUS))
            response = executeTopicStatus(commandLine, options, null);
        else if (cmd.commandName().equalsIgnoreCase(C_CON))
            response = executeConsumerCon(commandLine, options, null);
        else if (cmd.commandName().equalsIgnoreCase(P_CON))
            response = executeProducerCon(commandLine, options, null);
        else if (cmd.commandName().equalsIgnoreCase(C_PRO))
            response = executeConsumerProgress(commandLine, options, null);

        if (response == null || response.isEmpty())
            return DATA_NULL;
        return response;
    }



    private static String[] parseSubArgs(String[] args) {
        if (args.length > 1) {
            String[] result = new String[args.length - 1];
            for (int i = 0; i < args.length - 1; i++) {
                result[i] = args[i + 1];
            }
            return result;
        }
        return null;
    }

    private static SubCommand findSubCommand(final String name) {
        for (SubCommand cmd : subCommandList) {
            if (cmd.commandName().toUpperCase().equals(name.toUpperCase())) {
                return cmd;
            }
        }

        return null;
    }

    /**
     * can change the tools package void to String
     *
     * @param commandLine
     * @param options
     * @param rpcHook
     * @return
     */
    private static String executeProducerCon(CommandLine commandLine, Options options, RPCHook rpcHook) {
        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt(rpcHook);

        defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));

        try {
            defaultMQAdminExt.start();

            String group = commandLine.getOptionValue('g').trim();
            String topic = commandLine.getOptionValue('t').trim();

            ProducerConnection pc = defaultMQAdminExt.examineProducerConnectionInfo(group, topic);

            int i = 1;
            StringBuilder builder = new StringBuilder();
            for (Connection conn : pc.getConnectionSet()) {
                builder.append(String.format("%04d  %-32s %-22s %-8s %s\r\n",//
                    i++,//
                    conn.getClientId(),//
                    conn.getClientAddr(),//
                    conn.getLanguage(),//
                    MQVersion.getVersionDesc(conn.getVersion())//
                ));
            }
            return builder.toString();
        } catch (Exception e) {
            e.printStackTrace();
            return EXE_ERROR + e.getMessage() + "\r\n";
        } finally {
            defaultMQAdminExt.shutdown();
        }
    }

    private static String executeTopicStatus(final CommandLine commandLine, final Options options, RPCHook rpcHook) {
        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt(rpcHook);

        defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));

        try {
            defaultMQAdminExt.start();

            String topic = commandLine.getOptionValue('t').trim();
            TopicStatsTable topicStatsTable = defaultMQAdminExt.examineTopicStats(topic);

            List<MessageQueue> mqList = new LinkedList<MessageQueue>();
            mqList.addAll(topicStatsTable.getOffsetTable().keySet());
            Collections.sort(mqList);
            StringBuilder builder = new StringBuilder();
            builder.append(String.format("%-32s  %-4s  %-20s  %-20s    %s\r\n",//
                "#Broker Name",//
                "#QID",//
                "#Min Offset",//
                "#Max Offset",//
                "#Last Updated" //
            ));

            for (MessageQueue mq : mqList) {
                TopicOffset topicOffset = topicStatsTable.getOffsetTable().get(mq);

                String humanTimestamp = "";
                if (topicOffset.getLastUpdateTimestamp() > 0) {
                    humanTimestamp = UtilAll.timeMillisToHumanString2(topicOffset.getLastUpdateTimestamp());
                }

                builder.append(String.format("%-32s  %-4d  %-20d  %-20d    %s\r\n",//
                    UtilAll.frontStringAtLeast(mq.getBrokerName(), 32),//
                    mq.getQueueId(),//
                    topicOffset.getMinOffset(),//
                    topicOffset.getMaxOffset(),//
                    humanTimestamp //
                ));
            }

            return builder.toString();
        } catch (Exception e) {
            e.printStackTrace();
            return EXE_ERROR + e.getMessage() + "\r\n";
        } finally {
            defaultMQAdminExt.shutdown();
        }
    }

    private static String executeConsumerProgress(CommandLine commandLine, Options options, RPCHook rpcHook) {
        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt(rpcHook);

        defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));

        try {
            defaultMQAdminExt.start();
            StringBuilder builder = new StringBuilder();
            if (commandLine.hasOption('g')) {
                String consumerGroup = commandLine.getOptionValue('g').trim();
                ConsumeStats consumeStats = defaultMQAdminExt.examineConsumeStats(consumerGroup);

                List<MessageQueue> mqList = new LinkedList<MessageQueue>();
                mqList.addAll(consumeStats.getOffsetTable().keySet());
                Collections.sort(mqList);

                builder.append(String.format("%-32s  %-32s  %-4s  %-20s  %-20s  %-20s  %s\r\n",//
                    "#Topic",//
                    "#Broker Name",//
                    "#QID",//
                    "#Broker Offset",//
                    "#Consumer Offset",//
                    "#Diff", //
                    "#LastTime"));

                long diffTotal = 0L;

                for (MessageQueue mq : mqList) {
                    OffsetWrapper offsetWrapper = consumeStats.getOffsetTable().get(mq);

                    long diff = offsetWrapper.getBrokerOffset() - offsetWrapper.getConsumerOffset();
                    diffTotal += diff;

                    String lastTime = "-";
                    try {
                        lastTime = UtilAll.formatDate(new Date(offsetWrapper.getLastTimestamp()), UtilAll.YYYY_MM_DD_HH_MM_SS);
                    } catch (Exception e) {
                        //
                    }
                    if (offsetWrapper.getLastTimestamp() > 0)
                        builder.append(String.format("%-32s  %-32s  %-4d  %-20d  %-20d  %-20d  %s\r\n",//
                            UtilAll.frontStringAtLeast(mq.getTopic(), 32),//
                            UtilAll.frontStringAtLeast(mq.getBrokerName(), 32),//
                            mq.getQueueId(),//
                            offsetWrapper.getBrokerOffset(),//
                            offsetWrapper.getConsumerOffset(),//
                            diff, //
                            lastTime//
                        ));
                }

                builder.append(String.format("Consume TPS: %s\r\n", consumeStats.getConsumeTps()));
                builder.append(String.format("Diff Total: %d\r\n", diffTotal));
                return builder.toString();
            } else
                return ERROR;

        } catch (Exception e) {
            e.printStackTrace();
            return EXE_ERROR + e.getMessage() + "\r\n";
        } finally {
            defaultMQAdminExt.shutdown();
        }
    }

    private static String executeConsumerCon(CommandLine commandLine, Options options, RPCHook rpcHook) {
        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt(rpcHook);

        defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));

        try {
            defaultMQAdminExt.start();

            String group = commandLine.getOptionValue('g').trim();

            ConsumerConnection cc = defaultMQAdminExt.examineConsumerConnectionInfo(group);
            StringBuilder builder = new StringBuilder();
            int i = 1;
            for (Connection conn : cc.getConnectionSet()) {
                builder.append(String.format("%03d  %-32s %-22s %-8s %s\r\n",//
                    i++,//
                    conn.getClientId(),//
                    conn.getClientAddr(),//
                    conn.getLanguage(),//
                    MQVersion.getVersionDesc(conn.getVersion())//
                ));
            }

            builder.append("\nBelow is subscription:");
            Iterator<Entry<String, SubscriptionData>> it = cc.getSubscriptionTable().entrySet().iterator();
            i = 1;
            while (it.hasNext()) {
                Entry<String, SubscriptionData> entry = it.next();
                SubscriptionData sd = entry.getValue();
                builder.append(String.format("%03d  Topic: %-40s SubExpression: %s\r\n",//
                    i++,//
                    sd.getTopic(),//
                    sd.getSubString()//
                ));
            }

            builder.append(String.format("ConsumeType: %s\r\n", cc.getConsumeType()));
            builder.append(String.format("MessageModel: %s\r\n", cc.getMessageModel()));
            builder.append(String.format("ConsumeFromWhere: %s\r\n", cc.getConsumeFromWhere()));
            return builder.toString();
        } catch (Exception e) {
            e.printStackTrace();
            return EXE_ERROR + e.getMessage() + "\r\n";
        } finally {
            defaultMQAdminExt.shutdown();
        }
    }

}
