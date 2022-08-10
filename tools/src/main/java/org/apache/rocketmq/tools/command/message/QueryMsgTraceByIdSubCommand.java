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
package org.apache.rocketmq.tools.command.message;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.rocketmq.client.QueryResult;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.trace.TraceType;
import org.apache.rocketmq.client.trace.TraceView;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.SubCommand;
import org.apache.rocketmq.tools.command.SubCommandException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class QueryMsgTraceByIdSubCommand implements SubCommand {

    @Override
    public Options buildCommandlineOptions(Options options) {
        Option opt = new Option("i", "msgId", true, "Message Id");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("t", "traceTopic", true, "The name value of message trace topic");
        opt.setRequired(false);
        options.addOption(opt);
        return options;
    }

    @Override
    public String commandDesc() {
        return "query a message trace";
    }

    @Override
    public String commandName() {
        return "queryMsgTraceById";
    }

    @Override
    public String commandAlias() {
        return "QueryMsgTraceById";
    }

    @Override
    public void execute(CommandLine commandLine, Options options, RPCHook rpcHook) throws SubCommandException {
        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt(rpcHook);
        defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));
        try {
            final String msgId = commandLine.getOptionValue('i').trim();
            String traceTopic = TopicValidator.RMQ_SYS_TRACE_TOPIC;
            if (commandLine.hasOption('t')) {
                traceTopic = commandLine.getOptionValue('t').trim();
            }
            this.queryTraceByMsgId(defaultMQAdminExt, traceTopic, msgId);
        } catch (Exception e) {
            throw new SubCommandException(this.getClass().getSimpleName() + "command failed", e);
        } finally {
            defaultMQAdminExt.shutdown();
        }
    }

    private void queryTraceByMsgId(final DefaultMQAdminExt admin, String traceTopic, String msgId)
        throws MQClientException, InterruptedException {
        admin.start();
        QueryResult queryResult = admin.queryMessage(traceTopic, msgId, 64, 0, System.currentTimeMillis());
        List<MessageExt> messageList = queryResult.getMessageList();
        List<TraceView> traceViews = new ArrayList<>();
        for (MessageExt message : messageList) {
            List<TraceView> traceView = TraceView.decodeFromTraceTransData(msgId, message);
            traceViews.addAll(traceView);
        }

        this.printMessageTrace(traceViews);
    }

    private void printMessageTrace(List<TraceView> traceViews) {
        Map<String, List<TraceView>> consumerTraceMap = new HashMap<>(16);
        for (TraceView traceView : traceViews) {
            if (traceView.getMsgType().equals(TraceType.Pub.name())) {
                System.out.printf("%-10s %-20s %-20s %-20s %-10s %-10s%n",
                    "#Type",
                    "#ProducerGroup",
                    "#ClientHost",
                    "#SendTime",
                    "#CostTimes",
                    "#Status"
                );
                System.out.printf("%-10s %-20s %-20s %-20s %-10s %-10s%n",
                    "Pub",
                    traceView.getGroupName(),
                    traceView.getClientHost(),
                    DateFormatUtils.format(traceView.getTimeStamp(), "yyyy-MM-dd HH:mm:ss"),
                    traceView.getCostTime() + "ms",
                    traceView.getStatus()
                );
                System.out.printf("\n");
            }
            if (traceView.getMsgType().equals(TraceType.SubAfter.name())) {
                String groupName = traceView.getGroupName();
                if (consumerTraceMap.containsKey(groupName)) {
                    consumerTraceMap.get(groupName).add(traceView);
                } else {
                    ArrayList<TraceView> views = new ArrayList<>();
                    views.add(traceView);
                    consumerTraceMap.put(groupName, views);
                }
            }
        }

        Iterator<String> consumers = consumerTraceMap.keySet().iterator();
        while (consumers.hasNext()) {
            System.out.printf("%-10s %-20s %-20s %-20s %-10s %-10s%n",
                "#Type",
                "#ConsumerGroup",
                "#ClientHost",
                "#ConsumerTime",
                "#CostTimes",
                "#Status"
            );
            List<TraceView> consumerTraces = consumerTraceMap.get(consumers.next());
            for (TraceView traceView : consumerTraces) {
                System.out.printf("%-10s %-20s %-20s %-20s %-10s %-10s%n",
                    "Sub",
                    traceView.getGroupName(),
                    traceView.getClientHost(),
                    DateFormatUtils.format(traceView.getTimeStamp(), "yyyy-MM-dd HH:mm:ss"),
                    traceView.getCostTime() + "ms",
                    traceView.getStatus()
                );
            }
            System.out.printf("\n");
        }
    }
}
