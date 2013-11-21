/**
 * Copyright (C) 2010-2013 Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.rocketmq.tools.command.message;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import com.alibaba.rocketmq.client.exception.MQBrokerException;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.UtilAll;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.remoting.common.RemotingHelper;
import com.alibaba.rocketmq.remoting.exception.RemotingException;
import com.alibaba.rocketmq.tools.admin.DefaultMQAdminExt;
import com.alibaba.rocketmq.tools.command.SubCommand;


/**
 * 根据消息Id查询消息
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-8-12
 */
public class QueryMsgByIdSubCommand implements SubCommand {

    @Override
    public String commandName() {
        return "queryMsgById";
    }


    @Override
    public String commandDesc() {
        return "Query Message by Id";
    }


    @Override
    public Options buildCommandlineOptions(Options options) {
        Option opt = new Option("i", "msgId", true, "Message Id");
        opt.setRequired(true);
        options.addOption(opt);

        return options;
    }


    public static void queryById(final DefaultMQAdminExt admin, final String msgId) throws MQClientException,
            RemotingException, MQBrokerException, InterruptedException, IOException {
        admin.start();
        MessageExt msg = admin.viewMessage(msgId);

        // 存储消息 body 到指定路径
        String bodyTmpFilePath = createBodyFile(msg);

        System.out.printf("%-20s %s\n",//
            "Topic:",//
            msg.getTopic()//
            );

        System.out.printf("%-20s %s\n",//
            "Tags:",//
            "[" + msg.getTags() + "]"//
        );

        System.out.printf("%-20s %s\n",//
            "Keys:",//
            "[" + msg.getKeys() + "]"//
        );

        System.out.printf("%-20s %d\n",//
            "Queue ID:",//
            msg.getQueueId()//
            );

        System.out.printf("%-20s %d\n",//
            "Queue Offset:",//
            msg.getQueueOffset()//
            );

        System.out.printf("%-20s %d\n",//
            "CommitLog Offset:",//
            msg.getCommitLogOffset()//
            );

        System.out.printf("%-20s %s\n",//
            "Born Timestamp:",//
            UtilAll.timeMillisToHumanString2(msg.getBornTimestamp())//
            );

        System.out.printf("%-20s %s\n",//
            "Store Timestamp:",//
            UtilAll.timeMillisToHumanString2(msg.getStoreTimestamp())//
            );

        System.out.printf("%-20s %s\n",//
            "Born Host:",//
            RemotingHelper.parseSocketAddressAddr(msg.getBornHost())//
            );

        System.out.printf("%-20s %s\n",//
            "Store Host:",//
            RemotingHelper.parseSocketAddressAddr(msg.getStoreHost())//
            );

        System.out.printf("%-20s %d\n",//
            "System Flag:",//
            msg.getSysFlag()//
            );

        System.out.printf("%-20s %s\n",//
            "Properties:",//
            msg.getProperties() != null ? msg.getProperties().toString() : ""//
        );

        System.out.printf("%-20s %s\n",//
            "Message Body Path:",//
            bodyTmpFilePath//
            );
    }


    @Override
    public void execute(CommandLine commandLine, Options options) {
        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt();

        defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));

        try {
            final String msgId = commandLine.getOptionValue('i').trim();
            queryById(defaultMQAdminExt, msgId);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            defaultMQAdminExt.shutdown();
        }
    }


    private static String createBodyFile(MessageExt msg) throws IOException {
        DataOutputStream dos = null;

        try {
            String bodyTmpFilePath = "/tmp/rocketmq/msgbodys";
            File file = new File(bodyTmpFilePath);
            if (!file.exists()) {
                file.mkdirs();
            }
            bodyTmpFilePath = bodyTmpFilePath + "/" + msg.getMsgId();
            dos = new DataOutputStream(new FileOutputStream(bodyTmpFilePath));
            dos.write(msg.getBody());
            return bodyTmpFilePath;
        }
        finally {
            if (dos != null)
                dos.close();
        }
    }
}
