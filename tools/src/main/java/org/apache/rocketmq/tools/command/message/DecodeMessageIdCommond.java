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

import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.message.MessageClientIDSetter;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageId;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.tools.command.SubCommand;
import org.apache.rocketmq.tools.command.SubCommandException;

public class DecodeMessageIdCommond implements SubCommand {
    @Override
    public String commandName() {
        return "decodeMessageId";
    }

    @Override
    public String commandDesc() {
        return "decode unique message ID";
    }

    @Override
    public Options buildCommandlineOptions(Options options) {
        Option opt = new Option("i", "messageId", true, "unique message ID");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("o", "isOffsetMessageId", true, "offset meessage ID");
        opt.setRequired(false);
        options.addOption(opt);
        return options;
    }

    @Override
    public void execute(final CommandLine commandLine, final Options options,
        RPCHook rpcHook) throws SubCommandException {
        String messageId = commandLine.getOptionValue('i').trim();
        System.out.printf("msgId = %s%n", messageId);

        if (!commandLine.hasOption('o')) {
            System.out.printf("ip = %s%n", MessageClientIDSetter.getIPStrFromID(messageId));
            System.out.printf("pid = %d%n", MessageClientIDSetter.getPidFromID(messageId));
            try {
                String date = UtilAll.formatDate(MessageClientIDSetter.getNearlyTimeFromID(messageId), UtilAll.YYYY_MM_DD_HH_MM_SS_SSS);
                System.out.printf("date = %s%n", date);
            } catch (Exception e) {
                throw new SubCommandException(this.getClass().getSimpleName() + " command failed", e);
            }
        } else {
            try {
                MessageId msgId = MessageDecoder.decodeMessageId(messageId);
                System.out.printf("storeHost = %s%n", msgId.getAddress());
                System.out.printf("offset = %d%n", msgId.getOffset());
            } catch (UnknownHostException e) {
                throw new SubCommandException(this.getClass().getSimpleName() + " command failed", e);
            }
        }
    }
}
