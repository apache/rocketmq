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
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.io.Reader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.tools.admin.MQAdminUtils;
import org.apache.rocketmq.tools.command.SubCommand;
import org.apache.rocketmq.tools.command.SubCommandException;

import static org.apache.rocketmq.tools.command.message.ExportMessageCommand.DEFAULT_EXPORT_DIRECTORY;

public class ImportMessageCommand implements SubCommand {
    public static final int BATCH_SEND_SIZE = 32;
    private DefaultMQProducer producer;
    private final Logger logger = LoggerFactory.getLogger(ImportMessageCommand.class);

    @Override
    public String commandName() {
        return "importMessage";
    }

    @Override
    public String commandDesc() {
        return "Print Message Detail";
    }

    @Override
    public Options buildCommandlineOptions(Options options) {
        Option opt = new Option("t", "topic ", true, "Topic name,will override topic in message file");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("b", "broker ", true, "Send message to target broker");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("i", "qid ", true, "Send message to target queue");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("m", "msgTraceEnable", true, "Message Trace Enable, Default: false");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("c", "charsetName ", true, "CharsetName(default: UTF-8, eg: UTF-8,GBK)");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("d", "directory", true, "Import directory(default:" + DEFAULT_EXPORT_DIRECTORY + "),file path format:./importDir/topic/brokerName/queueId");
        opt.setRequired(false);
        options.addOption(opt);

        return options;
    }

    @Override
    public void execute(CommandLine commandLine, Options options, RPCHook rpcHook) throws SubCommandException {

        try {
            String charsetName = !commandLine.hasOption('c') ? "UTF-8" : commandLine.getOptionValue('c').trim();
            String topic = !commandLine.hasOption('t') ? null : commandLine.getOptionValue('t').trim();
            String brokerName = !commandLine.hasOption('b') ? null : commandLine.getOptionValue('b').trim();
            int queueId = !commandLine.hasOption('i') ? -1 : Integer.parseInt(commandLine.getOptionValue('i').trim());
            boolean msgTraceEnable = !commandLine.hasOption('m') ? false : Boolean.parseBoolean(commandLine.getOptionValue('m').trim());
            String directory =
                !commandLine.hasOption('d') ? DEFAULT_EXPORT_DIRECTORY : commandLine.getOptionValue('d').trim();

            producer = createProducer(rpcHook, msgTraceEnable);
            File dir = new File(directory);
            String[] topics = dir.list();
            if (topics == null || topics.length == 0) {
                return;
            }
            producer.start();

            for (String topicFileName : topics) {
                String topicPath = directory + File.separator + topicFileName;
                String[] brokerNames = new File(topicPath).list();
                if (brokerNames == null || brokerNames.length == 0) {
                    break;
                }
                for (String brokerFileName : brokerNames) {
                    File brokerDir = new File(topicPath + File.separator + brokerFileName);
                    File[] queueFiles = brokerDir.listFiles();
                    if (queueFiles == null || queueFiles.length == 0) {
                        break;
                    }
                    for (File queueFile : queueFiles) {
                        //get line numbers
                        int total;
                        try (Reader fileReader = new InputStreamReader(new FileInputStream(queueFile), charsetName)) {
                            LineNumberReader lineNumberReader = new LineNumberReader(fileReader);
                            lineNumberReader.skip(queueFile.length());
                            total = lineNumberReader.getLineNumber();
                            lineNumberReader.close();
                        }
                        if (total == 0) {
                            continue;
                        }

                        System.out.printf("import queueFile=%s%n", queueFile.getAbsolutePath());
                        List<String> lines = new ArrayList<>(BATCH_SEND_SIZE);
                        int sendSuccessCount = 0;
                        MQAdminUtils.printProgressWithFixedWidth(total, sendSuccessCount);
                        SendResult sendResult;
                        try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(queueFile), charsetName))) {
                            String msgStr;
                            do {
                                msgStr = reader.readLine();
                                if (msgStr != null) {
                                    lines.add(msgStr);
                                }
                                if (lines.size() % BATCH_SEND_SIZE == 0 || msgStr == null) {
                                    List<Message> messages = parseMessages(lines, topic, charsetName);
                                    // send to specified brokerName and queueId
                                    if (brokerName != null && queueId > -1) {
                                        MessageQueue messageQueue = new MessageQueue(topic, brokerName, queueId);
                                        sendResult = producer.send(messages, messageQueue);
                                    } else {
                                        sendResult = producer.send(messages);
                                    }
                                    if (sendResult.getSendStatus() == SendStatus.SEND_OK || sendResult.getSendStatus() == SendStatus.SLAVE_NOT_AVAILABLE) {
                                        sendSuccessCount += messages.size();
                                    } else {
                                        logger.error("fail send message, sendResult=" + sendResult);
                                    }
                                    MQAdminUtils.printProgressWithFixedWidth(total, sendSuccessCount);
                                    lines.clear();
                                }
                            }
                            while (msgStr != null);
                            // new line for printProgressWithFixedWidth
                            System.out.printf("%n");
                        } catch (IOException e) {
                            System.out.printf("read file failed, file=%s%n", queueFile.getAbsolutePath());
                        }
                    }
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(this.getClass().getSimpleName() + " command failed", e);
        } finally {
            producer.shutdown();
        }
    }

    private static List<Message> parseMessages(List<String> lines, String topic,
        String charsetName) throws SubCommandException {
        List<Message> messages = new ArrayList<>(lines.size());
        for (String line : lines) {
            JSONObject jsonObject = JSON.parseObject(line);
            Message m = new Message();
            // topic from command , override topic in message
            if (StringUtils.isNotBlank(topic)) {
                m.setTopic(topic);
            } else {
                m.setTopic((String) jsonObject.get("topic"));
            }
            m.setFlag((int) jsonObject.get("flag"));
            MessageAccessor.setProperties(m, (Map<String, String>) jsonObject.get("properties"));
            m.setTransactionId((String) jsonObject.get("transactionId"));
            String bodyFormat = (String) jsonObject.get("bodyFormat");
            Object body = jsonObject.get("body");
            m.setBody(parseBody(body, bodyFormat, charsetName));
            messages.add(m);
        }
        return messages;
    }

    private static byte[] parseBody(Object body, String bodyFormat, String charsetName) throws SubCommandException {
        switch (bodyFormat) {
            case "base64":
                return Base64.getDecoder().decode((String) body);
            case "json":
                return JSON.toJSONBytes(body);
            case "string":
                return ((String) body).getBytes(Charset.forName(charsetName));
            default:
                throw new SubCommandException("bodyFormat not supported! bodyFormat=" + bodyFormat);
        }
    }

    private DefaultMQProducer createProducer(RPCHook rpcHook, boolean msgTraceEnable) {
        if (this.producer != null) {
            return producer;
        } else {
            producer = new DefaultMQProducer(null, rpcHook, msgTraceEnable, null);
            producer.setProducerGroup(Long.toString(System.currentTimeMillis()));
            return producer;
        }
    }

}