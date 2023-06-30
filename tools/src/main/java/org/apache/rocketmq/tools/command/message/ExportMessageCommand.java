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

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Sets;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.tools.admin.MQAdminUtils;
import org.apache.rocketmq.tools.command.SubCommand;
import org.apache.rocketmq.tools.command.SubCommandException;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class ExportMessageCommand implements SubCommand {
    public static final String DEFAULT_EXPORT_DIRECTORY = "./rocketmq-export";
    private DefaultMQPullConsumer defaultMQPullConsumer;

    @Override
    public String commandName() {
        return "exportMessage";
    }

    @Override
    public String commandDesc() {
        return "Export Message";
    }

    @Override
    public Options buildCommandlineOptions(Options options) {
        Option opt = new Option("t", "topic ", true, "topic name");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("a", "brokerName ", true, "broker name");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("i", "queueId ", true, "queue id");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("c", "charsetName ", true, "CharsetName(eg: UTF-8,GBK)");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("s", "subExpression ", true, "Subscribe Expression(eg: TagA || TagB)");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("b", "beginTimestamp ", true, "Begin timestamp[currentTimeMillis|yyyy-MM-dd#HH:mm:ss:SSS]");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("e", "endTimestamp ", true, "End timestamp[currentTimeMillis|yyyy-MM-dd#HH:mm:ss:SSS]");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("d", "directory ", true, "Export directory(default:" + DEFAULT_EXPORT_DIRECTORY + "),file path format:./exportDir/topic/brokerName/queueId");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("f", "format ", true, "message body format[base64|json|string],default:base64");
        opt.setRequired(false);
        options.addOption(opt);

        return options;
    }

    @Override
    public void execute(CommandLine commandLine, Options options, RPCHook rpcHook) throws SubCommandException {
        if (defaultMQPullConsumer == null) {
            defaultMQPullConsumer = new DefaultMQPullConsumer(MixAll.TOOLS_CONSUMER_GROUP, rpcHook);
        }
        try {
            String charsetName =
                !commandLine.hasOption('c') ? "UTF-8" : commandLine.getOptionValue('c').trim();

            String subExpression =
                !commandLine.hasOption('s') ? "*" : commandLine.getOptionValue('s').trim();
            String topic = commandLine.getOptionValue('t').trim();

            String brokerName = !commandLine.hasOption('a') ? null : commandLine.getOptionValue('a').trim();
            int queueId = !commandLine.hasOption('i') ? -1 : Integer.parseInt(commandLine.getOptionValue('i').trim());
            if (StringUtils.isBlank(brokerName) && queueId != -1) {
                throw new SubCommandException("Please set the brokerName before queueId!");
            }
            String directory =
                !commandLine.hasOption('d') ? DEFAULT_EXPORT_DIRECTORY : commandLine.getOptionValue('s').trim();
            String bodyFormat =
                !commandLine.hasOption('f') ? "base64" : commandLine.getOptionValue('f').trim();

            defaultMQPullConsumer.start();
            Set<MessageQueue> messageQueues = Collections.emptySet();
            if (StringUtils.isNotBlank(brokerName) && queueId != -1) {
                messageQueues = Sets.newHashSet(new MessageQueue(topic, brokerName, queueId));
            } else if (StringUtils.isNotBlank(brokerName) && queueId == -1) {
                messageQueues = defaultMQPullConsumer.fetchSubscribeMessageQueues(topic).stream().filter(e -> e.getBrokerName().equals(brokerName)).collect(Collectors.toSet());
            } else if (StringUtils.isBlank(brokerName) && queueId == -1) {
                messageQueues = defaultMQPullConsumer.fetchSubscribeMessageQueues(topic);
            }

            String topicDir = directory + File.separator + topic;
            FileUtils.forceDeleteOnExit(new File(topicDir));
            FileUtils.forceMkdirParent(new File(topicDir));
            for (MessageQueue mq : messageQueues) {
                String brokerDir = topicDir + File.separator + mq.getBrokerName();
                String queueFilepath = brokerDir + File.separator + mq.getQueueId();
                File brokerDirFile = new File(brokerDir);
                FileUtils.forceMkdir(brokerDirFile);
                File queueFile = new File(queueFilepath);
                System.out.println("exporrt queueFile queueFilepath=" + queueFile.getAbsoluteFile());

                long minOffset = defaultMQPullConsumer.minOffset(mq);
                long maxOffset = defaultMQPullConsumer.maxOffset(mq);

                if (commandLine.hasOption('b')) {
                    String timestampStr = commandLine.getOptionValue('b').trim();
                    long timeValue = timestampFormat(timestampStr);
                    minOffset = defaultMQPullConsumer.searchOffset(mq, timeValue);
                }

                if (commandLine.hasOption('e')) {
                    String timestampStr = commandLine.getOptionValue('e').trim();
                    long timeValue = timestampFormat(timestampStr);
                    maxOffset = defaultMQPullConsumer.searchOffset(mq, timeValue);
                }

                System.out.printf("export %s minOffset=%s, maxOffset=%s%n", minOffset, maxOffset, mq);
                try (BufferedWriter writer = new BufferedWriter(new FileWriter(queueFile))) {
                    READQ:
                    for (long offset = minOffset; offset < maxOffset; ) {
                        try {
                            PullResult pullResult = defaultMQPullConsumer.pull(mq, subExpression, offset, 128);
                            offset = pullResult.getNextBeginOffset();
                            switch (pullResult.getPullStatus()) {
                                case FOUND:
                                    MQAdminUtils.printProgressWithFixedWidth(maxOffset - minOffset, offset - minOffset);
                                    exportMessage(writer, pullResult.getMsgFoundList(), charsetName, bodyFormat);
                                    break;
                                case NO_MATCHED_MSG:
                                    System.out.printf("%s no matched msg. status=%s, offset=%s%n", mq, pullResult.getPullStatus(), offset);
                                    break;
                                case NO_NEW_MSG:
                                case OFFSET_ILLEGAL:
                                    System.out.printf("%s print msg finished. status=%s, offset=%s%n", mq, pullResult.getPullStatus(), offset);
                                    break READQ;
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                            break;
                        }
                    }
                }
                // new line for printProgressWithFixedWidth
                System.out.println();
            }
        } catch (Exception e) {
            throw new SubCommandException(this.getClass().getSimpleName() + " command failed", e);
        } finally {
            defaultMQPullConsumer.shutdown();
        }
    }

    private static void exportMessage(BufferedWriter writer, List<MessageExt> msgFoundList, String charsetName,
        String bodyFormat) throws IOException, SubCommandException {
        for (MessageExt messageExt : msgFoundList) {
            JSONObject jsonObject = new JSONObject();
            jsonObject.put("topic", messageExt.getTopic());
            jsonObject.put("flag", messageExt.getFlag());
            jsonObject.put("properties", messageExt.getProperties());
            jsonObject.put("transactionId", messageExt.getTransactionId());
            jsonObject.put("msgId", messageExt.getMsgId());
            jsonObject.put("queueOffset", messageExt.getQueueOffset());
            jsonObject.put("bodyFormat", bodyFormat);
            jsonObject.put("body", formatBody(messageExt.getBody(), bodyFormat, charsetName));
            writer.write(JSON.toJSONString(jsonObject));
            writer.newLine();
        }
    }

    public static Object formatBody(byte[] body, String bodyFormat, String charsetName) throws SubCommandException {
        switch (bodyFormat) {
            case "base64":
                // fastjson default use base64 to encode
                return body;
            case "json":
                return JSON.parseObject(body, Object.class);
            case "string":
                return new String(body, Charset.forName(charsetName));
            default:
                throw new SubCommandException("bodyFormat not supported! bodyFormat=" + bodyFormat);
        }
    }

    private static long timestampFormat(final String value) {
        long timestamp;
        try {
            timestamp = Long.parseLong(value);
        } catch (NumberFormatException e) {

            timestamp = UtilAll.parseDate(value, UtilAll.YYYY_MM_DD_HH_MM_SS_SSS).getTime();
        }

        return timestamp;
    }

}
