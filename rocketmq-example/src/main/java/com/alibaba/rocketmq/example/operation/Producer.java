/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.alibaba.rocketmq.example.operation;

import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.common.message.Message;
import com.alibaba.rocketmq.remoting.common.RemotingHelper;
import org.apache.commons.cli.*;

public class Producer {

    public static void main(String[] args) throws MQClientException, InterruptedException {
        CommandLine commandLine = buildCommandline(args);
        if (commandLine != null) {
            String group = commandLine.getOptionValue('g');
            String topic = commandLine.getOptionValue('t');
            String tags = commandLine.getOptionValue('a');
            String keys = commandLine.getOptionValue('k');
            String msgCount = commandLine.getOptionValue('c');

            DefaultMQProducer producer = new DefaultMQProducer(group);
            producer.setInstanceName(Long.toString(System.currentTimeMillis()));

            producer.start();

            for (int i = 0; i < Integer.parseInt(msgCount); i++) {
                try {
                    Message msg = new Message(//
                            topic,// topic
                            tags,// tag
                            keys,// key
                            ("Hello RocketMQ " + i).getBytes(RemotingHelper.DEFAULT_CHARSET));// body
                    SendResult sendResult = producer.send(msg);

                    System.out.printf("%-8d %s%n", i, sendResult);
                } catch (Exception e) {
                    e.printStackTrace();
                    Thread.sleep(1000);
                }
            }

            producer.shutdown();
        }
    }

    public static CommandLine buildCommandline(String[] args) {
        final Options options = new Options();
        // ////////////////////////////////////////////////////
        Option opt = new Option("h", "help", false, "Print help");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("g", "producerGroup", true, "Producer Group Name");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("t", "topic", true, "Topic Name");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("a", "tags", true, "Tags Name");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("k", "keys", true, "Keys Name");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("c", "msgCount", true, "Message Count");
        opt.setRequired(true);
        options.addOption(opt);

        // ////////////////////////////////////////////////////

        PosixParser parser = new PosixParser();
        HelpFormatter hf = new HelpFormatter();
        hf.setWidth(110);
        CommandLine commandLine = null;
        try {
            commandLine = parser.parse(options, args);
            if (commandLine.hasOption('h')) {
                hf.printHelp("producer", options, true);
                return null;
            }
        } catch (ParseException e) {
            hf.printHelp("producer", options, true);
            return null;
        }

        return commandLine;
    }
}
