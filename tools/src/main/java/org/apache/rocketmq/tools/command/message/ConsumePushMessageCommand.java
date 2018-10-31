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

import java.util.List;
import java.util.concurrent.CountDownLatch;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.tools.command.SubCommand;
import org.apache.rocketmq.tools.command.SubCommandException;


public class ConsumePushMessageCommand implements SubCommand {

    private DefaultMQPushConsumer defaultMQPushConsumer;

    public enum ConsumeType {
        /**
         * Topic only
         */
        DEFAULT,
        /**
         * Topic brokerName queueId set
         */
        BYQUEUE,
        /**
         * Topic brokerName queueId offset set
         */
        BYOFFSET
    }

    @Override
    public String commandName() {
        return "ConsumeMessagePush";
    }

    @Override
    public String commandDesc() {
        return "Consume message by push";
    }

    @Override
    public Options buildCommandlineOptions(final Options options) {
        Option opt = new Option("t", "topic", true, "Topic name");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("g", "consumerGroup", true, "Consumer group name");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("n", "nameServer", true, "NameSrv addr");
        opt.setRequired(true);
        options.addOption(opt);
        return options;

    }

    @Override
    public void execute(final CommandLine commandLine, final Options options, RPCHook rpcHook) throws SubCommandException {
        try {
            /* Group name must be set before consumer start */
            final CountDownLatch countDownLatch = new CountDownLatch(1);
            defaultMQPushConsumer = new DefaultMQPushConsumer(commandLine.getOptionValue('g').trim());
            defaultMQPushConsumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
            defaultMQPushConsumer.setNamesrvAddr(commandLine.getOptionValue('n').trim());
            defaultMQPushConsumer.subscribe(commandLine.getOptionValue('t').trim(), "*");
            defaultMQPushConsumer.registerMessageListener(new MessageListenerConcurrently() {
                @Override
                public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
                    ConsumeConcurrentlyContext context) {
                    System.out.printf("%s Receive New Messages: %s %n", Thread.currentThread().getName(), msgs);
                    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                }
            });
            defaultMQPushConsumer.start();
            System.out.printf("Consumer Started.%n");

            countDownLatch.await();
        } catch (Exception e) {
            throw new SubCommandException(this.getClass().getSimpleName() + " command failed", e);
        } finally {
            defaultMQPushConsumer.shutdown();
        }
    }
}