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

package org.apache.rocketmq.tools.command.offset;

import java.util.Map;
import java.util.Objects;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.SubCommand;
import org.apache.rocketmq.tools.command.SubCommandException;

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

        opt = new Option("s", "timestamp", true, "set the timestamp[now|currentTimeMillis|yyyy-MM-dd#HH:mm:ss:SSS]");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("f", "force", true, "set the force rollback by timestamp switch[true|false]. Deprecated.");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("c", "cplus", false, "reset c++ client offset. Deprecated.");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("b", "broker", true, "broker addr");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("q", "queue", true, "queue id");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("o", "offset", true, "Expect queue offset, not support old version broker");
        opt.setRequired(false);
        options.addOption(opt);

        return options;
    }

    @Override
    public void execute(CommandLine commandLine, Options options, RPCHook rpcHook) throws SubCommandException {
        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt(rpcHook);
        defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));
        try {
            String group = commandLine.getOptionValue("g").trim();
            String topic = commandLine.getOptionValue("t").trim();
            String timeStampStr = commandLine.getOptionValue("s").trim();
            long timestamp = "now".equals(timeStampStr) ? System.currentTimeMillis() : 0;

            try {
                if (timestamp == 0) {
                    timestamp = Long.parseLong(timeStampStr);
                }
            } catch (NumberFormatException e) {
                timestamp = Objects.requireNonNull(
                    UtilAll.parseDate(timeStampStr, UtilAll.YYYY_MM_DD_HH_MM_SS_SSS)).getTime();
            }

            boolean force = true;
            if (commandLine.hasOption('f')) {
                force = Boolean.parseBoolean(commandLine.getOptionValue("f").trim());
            }

            boolean isC = commandLine.hasOption('c');

            String brokerAddr = null;
            if (commandLine.hasOption('b')) {
                brokerAddr = commandLine.getOptionValue("b");
            }
            int queueId = -1;
            if (commandLine.hasOption("q")) {
                queueId = Integer.parseInt(commandLine.getOptionValue('q'));
            }

            if (commandLine.hasOption('n')) {
                defaultMQAdminExt.setNamesrvAddr(commandLine.getOptionValue('n').trim());
            }

            Long offset = null;
            if (commandLine.hasOption('o')) {
                offset = Long.parseLong(commandLine.getOptionValue('o'));
            }

            defaultMQAdminExt.start();

            if (brokerAddr != null && queueId >= 0) {
                System.out.printf("start reset consumer offset by specified, " +
                        "group[%s], topic[%s], queueId[%s], broker[%s], timestamp(string)[%s], timestamp(long)[%s]%n",
                        group, topic, queueId, brokerAddr, timeStampStr, timestamp);

                long resetOffset = null != offset ? offset :
                    defaultMQAdminExt.searchOffset(brokerAddr, topic, queueId, timestamp, 3000);

                System.out.printf("reset consumer offset to %d%n", resetOffset);
                if (resetOffset > 0) {
                    defaultMQAdminExt.resetOffsetByQueueId(brokerAddr, group, topic, queueId, resetOffset);
                }
                return;
            }

            Map<MessageQueue, Long> offsetTable;
            try {
                offsetTable = defaultMQAdminExt.resetOffsetByTimestamp(topic, group, timestamp, force, isC);
            } catch (MQClientException e) {
                // if consumer not online, use old command to reset reset
                if (ResponseCode.CONSUMER_NOT_ONLINE == e.getResponseCode()) {
                    ResetOffsetByTimeOldCommand.resetOffset(defaultMQAdminExt, group, topic, timestamp, force, timeStampStr);
                    return;
                }
                throw e;
            }

            System.out.printf("start reset consumer offset by specified, " +
                    "group[%s], topic[%s], force[%s], timestamp(string)[%s], timestamp(long)[%s]%n",
                group, topic, force, timeStampStr, timestamp);

            System.out.printf("%-40s  %-40s  %-40s%n", "#brokerName", "#queueId", "#offset");

            for (Map.Entry<MessageQueue, Long> entry : offsetTable.entrySet()) {
                System.out.printf("%-40s  %-40d  %-40d%n",
                    UtilAll.frontStringAtLeast(entry.getKey().getBrokerName(), 32),
                    entry.getKey().getQueueId(),
                    entry.getValue());
            }
        } catch (Exception e) {
            throw new SubCommandException(this.getClass().getSimpleName() + " command failed", e);
        } finally {
            defaultMQAdminExt.shutdown();
        }
    }
}
