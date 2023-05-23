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
package org.apache.rocketmq.broker.transaction.queue;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.common.topic.TopicValidator;

public class TransactionalMessageUtil {
    public static final String REMOVE_TAG = "d";
    public static final Charset CHARSET = StandardCharsets.UTF_8;
    public static final String OFFSET_SEPARATOR = ",";
    public static final String TRANSACTION_ID = "__transactionId__";

    public static String buildOpTopic() {
        return TopicValidator.RMQ_SYS_TRANS_OP_HALF_TOPIC;
    }

    public static String buildHalfTopic() {
        return TopicValidator.RMQ_SYS_TRANS_HALF_TOPIC;
    }

    public static String buildConsumerGroup() {
        return MixAll.CID_SYS_RMQ_TRANS;
    }

    public static MessageExtBrokerInner buildTransactionalMessageFromHalfMessage(MessageExt msgExt) {
        final MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
        msgInner.setWaitStoreMsgOK(false);
        msgInner.setMsgId(msgExt.getMsgId());
        msgInner.setTopic(msgExt.getProperty(MessageConst.PROPERTY_REAL_TOPIC));
        msgInner.setBody(msgExt.getBody());
        final String realQueueIdStr = msgExt.getProperty(MessageConst.PROPERTY_REAL_QUEUE_ID);
        if (StringUtils.isNumeric(realQueueIdStr)) {
            msgInner.setQueueId(Integer.parseInt(realQueueIdStr));
        }
        msgInner.setFlag(msgExt.getFlag());
        msgInner.setTagsCode(MessageExtBrokerInner.tagsString2tagsCode(msgInner.getTags()));
        msgInner.setBornTimestamp(msgExt.getBornTimestamp());
        msgInner.setBornHost(msgExt.getBornHost());
        msgInner.setTransactionId(msgExt.getProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX));

        MessageAccessor.setProperties(msgInner, msgExt.getProperties());
        MessageAccessor.putProperty(msgInner, MessageConst.PROPERTY_TRANSACTION_PREPARED, "true");
        MessageAccessor.clearProperty(msgInner, MessageConst.PROPERTY_TRANSACTION_PREPARED_QUEUE_OFFSET);
        MessageAccessor.clearProperty(msgInner, MessageConst.PROPERTY_REAL_QUEUE_ID);
        msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgInner.getProperties()));

        int sysFlag = msgExt.getSysFlag();
        sysFlag |= MessageSysFlag.TRANSACTION_PREPARED_TYPE;
        msgInner.setSysFlag(sysFlag);

        return msgInner;
    }

    public static long getImmunityTime(String checkImmunityTimeStr, long transactionTimeout) {
        long checkImmunityTime = 0;

        try {
            checkImmunityTime = Long.parseLong(checkImmunityTimeStr) * 1000;
        } catch (Throwable ignored) {
        }

        //If a custom first check time is set, the minimum check time;
        //The default check protection period is transactionTimeout
        if (checkImmunityTime < transactionTimeout) {
            checkImmunityTime = transactionTimeout;
        }
        return checkImmunityTime;
    }
}
