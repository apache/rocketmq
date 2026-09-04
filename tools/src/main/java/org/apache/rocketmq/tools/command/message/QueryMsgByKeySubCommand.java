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
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.QueryResult;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.exception.RemotingException;

import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.SubCommand;
import org.apache.rocketmq.tools.command.SubCommandException;

public class QueryMsgByKeySubCommand implements SubCommand {

    @Override
    public String commandName() {
        return "queryMsgByKey";
    }

    @Override
    public String commandDesc() {
        return "Query Message by Key.";
    }

    @Override
    public Options buildCommandlineOptions(Options options) {
        Option opt = new Option("t", "topic", true, "Topic name");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("k", "msgKey", true, "Message Key");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("b", "beginTimestamp", true, "Begin timestamp(ms). default:0, eg:1676730526212");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("e", "endTimestamp", true, "End timestamp(ms). default:Long.MAX_VALUE, eg:1676730526212");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("m", "maxNum", true, "The maximum number of messages returned by the query, default:64");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("c", "cluster", true, "Cluster name or lmq parent topic, lmq is used to find the route.");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("p", "keyType", true, "Index key type, default index key type is K, you can use K for keys OR T for tags");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("l", "lastKey", true, "Last Key");
        opt.setRequired(false);
        options.addOption(opt);

        return options;
    }

    @Override
    public void execute(CommandLine commandLine, Options options, RPCHook rpcHook) throws SubCommandException {
        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt(rpcHook);

        defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));

        try {
            final String topic = commandLine.getOptionValue('t').trim();
            final String key = commandLine.getOptionValue('k').trim();
            String keyType = MessageConst.INDEX_KEY_TYPE;
            String lastKey = null;
            long beginTimestamp = 0;
            long endTimestamp = Long.MAX_VALUE;
            int maxNum = 64;
            String clusterName = null;
            if (commandLine.hasOption("b")) {
                beginTimestamp = Long.parseLong(commandLine.getOptionValue("b").trim());
            }
            if (commandLine.hasOption("e")) {
                endTimestamp = Long.parseLong(commandLine.getOptionValue("e").trim());
            }
            if (commandLine.hasOption("m")) {
                maxNum = Integer.parseInt(commandLine.getOptionValue("m").trim());
            }
            if (commandLine.hasOption("c")) {
                clusterName = commandLine.getOptionValue("c").trim();
            }
            if (commandLine.hasOption("p")) {
                keyType = commandLine.getOptionValue("p").trim();
                if (StringUtils.isEmpty(keyType) || !MessageConst.INDEX_KEY_TYPE.equals(keyType) && !MessageConst.INDEX_TAG_TYPE.equals(keyType)) {
                    System.out.printf("index type error, just support K for keys or T for tags");
                    return;
                }
            }
            if (commandLine.hasOption("l")) {
                lastKey = commandLine.getOptionValue("l").trim();
            }
            this.queryByKey(defaultMQAdminExt, clusterName, topic, key, maxNum, beginTimestamp, endTimestamp, keyType, lastKey);
        } catch (Exception e) {
            throw new SubCommandException(this.getClass().getSimpleName() + " command failed", e);
        } finally {
            defaultMQAdminExt.shutdown();
        }
    }

    private void queryByKey(final DefaultMQAdminExt admin, final String cluster, final String topic, final String key, int maxNum, long begin,
        long end, String keyType, String lastKey)
        throws MQClientException, InterruptedException, RemotingException {
        admin.start();
        QueryResult queryResult = admin.queryMessage(cluster, topic, key, maxNum, begin, end, keyType, lastKey);
        System.out.printf("%-50s %4s %40s %-200s%n",
            "#Message ID",
            "#QID",
            "#Offset",
            "#IndexKey");
        for (MessageExt msg : queryResult.getMessageList()) {
            if (!StringUtils.isEmpty(keyType)) {
                long storeTimestamp = MixAll.dealTimeToHourStamps(msg.getStoreTimestamp());
                String indexLastKey = storeTimestamp + "@" + topic + "@" + keyType + "@" + key + "@" + msg.getMsgId() + "@" + msg.getCommitLogOffset();
                System.out.printf("%-50s %4d %40d %-200s%n", msg.getMsgId(), msg.getQueueId(), msg.getQueueOffset(), indexLastKey);
            } else {
                System.out.printf("%-50s %4d %40d%n", msg.getMsgId(), msg.getQueueId(), msg.getQueueOffset());
            }
        }
    }
}
