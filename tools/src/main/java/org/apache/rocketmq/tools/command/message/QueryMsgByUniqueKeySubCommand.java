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

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.client.QueryResult;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.remoting.protocol.body.ConsumeMessageDirectlyResult;
import org.apache.rocketmq.remoting.protocol.body.ConsumerRunningInfo;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.admin.api.MessageTrack;
import org.apache.rocketmq.tools.command.SubCommand;
import org.apache.rocketmq.tools.command.SubCommandException;

public class QueryMsgByUniqueKeySubCommand implements SubCommand {

    private DefaultMQAdminExt defaultMQAdminExt;

    private DefaultMQAdminExt createMQAdminExt(RPCHook rpcHook) throws SubCommandException {
        if (this.defaultMQAdminExt != null) {
            return defaultMQAdminExt;
        } else {
            defaultMQAdminExt = new DefaultMQAdminExt(rpcHook);
            defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));
            try {
                defaultMQAdminExt.start();
            }
            catch (Exception e) {
                throw new SubCommandException(this.getClass().getSimpleName() + " command failed", e);
            }
            return defaultMQAdminExt;
        }
    }

    public static void queryById(final DefaultMQAdminExt admin, final String topic, final String msgId,
                                 final boolean showAll) throws MQClientException,
            RemotingException, MQBrokerException, InterruptedException, IOException {

        QueryResult queryResult = admin.queryMessageByUniqKey(topic, msgId, 32, 0, Long.MAX_VALUE);
        assert queryResult != null;
        List<MessageExt> list = queryResult.getMessageList();
        if (list == null || list.size() == 0) {
            return;
        }
        list.sort((o1, o2) -> (int) (o1.getStoreTimestamp() - o2.getStoreTimestamp()));
        for (int i = 0; i < (showAll ? list.size() : 1); i++) {
            showMessage(admin, list.get(i), i);
        }
    }

    private static void showMessage(final DefaultMQAdminExt admin, MessageExt msg, int index) throws IOException {
        String bodyTmpFilePath = createBodyFile(msg, index);

        final String strFormat = "%-20s %s%n";
        final String intFormat = "%-20s %d%n";

        System.out.printf(strFormat, "Topic:", msg.getTopic());
        System.out.printf(strFormat, "Tags:", "[" + msg.getTags() + "]");
        System.out.printf(strFormat, "Keys:", "[" + msg.getKeys() + "]");
        System.out.printf(intFormat, "Queue ID:", msg.getQueueId());
        System.out.printf(intFormat, "Queue Offset:", msg.getQueueOffset());
        System.out.printf(intFormat, "CommitLog Offset:", msg.getCommitLogOffset());
        System.out.printf(intFormat, "Reconsume Times:", msg.getReconsumeTimes());
        System.out.printf(strFormat, "Born Timestamp:", UtilAll.timeMillisToHumanString2(msg.getBornTimestamp()));
        System.out.printf(strFormat, "Store Timestamp:", UtilAll.timeMillisToHumanString2(msg.getStoreTimestamp()));
        System.out.printf(strFormat, "Born Host:", RemotingHelper.parseSocketAddressAddr(msg.getBornHost()));
        System.out.printf(strFormat, "Store Host:", RemotingHelper.parseSocketAddressAddr(msg.getStoreHost()));
        System.out.printf(intFormat, "System Flag:", msg.getSysFlag());
        System.out.printf(strFormat, "Properties:",
                msg.getProperties() != null ? msg.getProperties().toString() : "");
        System.out.printf(strFormat, "Message Body Path:", bodyTmpFilePath);

        try {
            List<MessageTrack> mtdList = admin.messageTrackDetail(msg);
            if (mtdList.isEmpty()) {
                System.out.printf("%n%nWARN: No Consumer");
            } else {
                System.out.printf("%n%n");
                for (MessageTrack mt : mtdList) {
                    System.out.printf("%s", mt);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static String createBodyFile(MessageExt msg, int index) throws IOException {
        DataOutputStream dos = null;
        try {
            StringBuilder bodyTmpFilePath = new StringBuilder("/tmp/rocketmq/msgbodys");
            File file = new File(bodyTmpFilePath.toString());
            if (!file.exists()) {
                file.mkdirs();
            }
            bodyTmpFilePath.append("/").append(msg.getMsgId());
            if (index > 0) {
                bodyTmpFilePath.append("_" + index);
            }
            dos = new DataOutputStream(new FileOutputStream(bodyTmpFilePath.toString()));
            dos.write(msg.getBody());
            return bodyTmpFilePath.toString();
        } finally {
            if (dos != null) {
                dos.close();
            }
        }
    }

    @Override
    public String commandName() {
        return "queryMsgByUniqueKey";
    }

    @Override
    public String commandDesc() {
        return "Query Message by Unique key";
    }

    @Override
    public Options buildCommandlineOptions(Options options) {
        Option opt = new Option("i", "msgId", true, "Message Id");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("g", "consumerGroup", true, "consumer group name");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("d", "clientId", true, "The consumer's client id");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("t", "topic", true, "The topic of msg");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("a", "showAll", false, "Print all message, the limit is 32");
        opt.setRequired(false);
        options.addOption(opt);

        return options;
    }

    @Override
    public void execute(CommandLine commandLine, Options options, RPCHook rpcHook) throws SubCommandException {

        try {
            defaultMQAdminExt =  createMQAdminExt(rpcHook);

            final String msgId = commandLine.getOptionValue('i').trim();
            final String topic = commandLine.getOptionValue('t').trim();
            final boolean showAll = commandLine.hasOption('a');
            if (commandLine.hasOption('g') && commandLine.hasOption('d')) {
                final String consumerGroup = commandLine.getOptionValue('g').trim();
                final String clientId = commandLine.getOptionValue('d').trim();
                ConsumerRunningInfo consumerRunningInfo = null;
                try {
                    consumerRunningInfo = defaultMQAdminExt.getConsumerRunningInfo(consumerGroup, clientId, false, false);
                } catch (Exception e) {
                    System.out.printf("get consumer runtime info for %s client failed \n", clientId);
                }
                if (consumerRunningInfo != null && ConsumerRunningInfo.isPushType(consumerRunningInfo)) {
                    ConsumeMessageDirectlyResult result =
                            defaultMQAdminExt.consumeMessageDirectly(consumerGroup, clientId, topic, msgId);
                    System.out.printf("%s", result);
                } else {
                    System.out.printf("get consumer info failed or this %s client is not push consumer ,not support direct push \n", clientId);
                }

            } else {
                queryById(defaultMQAdminExt, topic, msgId, showAll);
            }
        } catch (Exception e) {
            throw new SubCommandException(this.getClass().getSimpleName() + " command failed", e);
        } finally {
            defaultMQAdminExt.shutdown();
        }
    }
}
