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
package org.apache.rocketmq.example.operation;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;

public class Consumer {

    public static void main(String[] args) throws MQClientException {
        CommandLine commandLine = buildCommandline(args);
        if (commandLine != null) {
            String subGroup = commandLine.getOptionValue('g');
            String topic = commandLine.getOptionValue('t');
            String subExpression = commandLine.getOptionValue('s');
            final String returnFailedHalf = commandLine.getOptionValue('f');

            DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(subGroup);
            consumer.setInstanceName(Long.toString(System.currentTimeMillis()));

            consumer.subscribe(topic, subExpression);

            consumer.registerMessageListener(new MessageListenerConcurrently() {
                AtomicLong consumeTimes = new AtomicLong(0);

                @Override
                public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
                    ConsumeConcurrentlyContext context) {
                    long currentTimes = this.consumeTimes.incrementAndGet();
                    System.out.printf("%-8d %s%n", currentTimes, msgs);
                    if (Boolean.parseBoolean(returnFailedHalf)) {
                        if ((currentTimes % 2) == 0) {
                            return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                        }
                    }
                    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                }
            });

            consumer.start();

            System.out.printf("Consumer Started.%n");
        }
    }

    public static CommandLine buildCommandline(String[] args) {
        final Options options = new Options();
        Option opt = new Option("h", "help", false, "Print help");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("g", "consumerGroup", true, "Consumer Group Name");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("t", "topic", true, "Topic Name");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("s", "subscription", true, "subscription");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("f", "returnFailedHalf", true, "return failed result, for half message");
        opt.setRequired(true);
        options.addOption(opt);

        DefaultParser parser = new DefaultParser();
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
