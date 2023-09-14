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


import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TransactionalMessageUtilTest {

    @Test
    public void testBuildTransactionalMessageFromHalfMessage() {
        MessageExt halfMessage = new MessageExt();
        halfMessage.setTopic(TransactionalMessageUtil.buildHalfTopic());
        MessageAccessor.putProperty(halfMessage, MessageConst.PROPERTY_REAL_TOPIC, "real-topic");
        halfMessage.setMsgId("msgId");
        halfMessage.setTransactionId("tranId");
        MessageAccessor.putProperty(halfMessage, MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX, "tranId");
        MessageAccessor.putProperty(halfMessage, MessageConst.PROPERTY_PRODUCER_GROUP, "trans-producer-grp");

        MessageExtBrokerInner msgExtInner = TransactionalMessageUtil.buildTransactionalMessageFromHalfMessage(halfMessage);


        assertEquals("real-topic", msgExtInner.getTopic());
        assertEquals("true", msgExtInner.getProperty(MessageConst.PROPERTY_TRANSACTION_PREPARED));
        assertEquals(msgExtInner.getProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX),
            halfMessage.getProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX));
        assertEquals(msgExtInner.getMsgId(), halfMessage.getMsgId());
        assertTrue(MessageSysFlag.check(msgExtInner.getSysFlag(), MessageSysFlag.TRANSACTION_PREPARED_TYPE));
        assertEquals(msgExtInner.getProperty(MessageConst.PROPERTY_PRODUCER_GROUP), halfMessage.getProperty(MessageConst.PROPERTY_PRODUCER_GROUP));
    }

    @Test
    public void testGetImmunityTime() {
        long transactionTimeout = 6 * 1000;

        String checkImmunityTimeStr = "1";
        long immunityTime = TransactionalMessageUtil.getImmunityTime(checkImmunityTimeStr, transactionTimeout);
        Assert.assertEquals(6 * 1000, immunityTime);

        checkImmunityTimeStr = "5";
        immunityTime = TransactionalMessageUtil.getImmunityTime(checkImmunityTimeStr, transactionTimeout);
        Assert.assertEquals(6 * 1000, immunityTime);

        checkImmunityTimeStr = "7";
        immunityTime = TransactionalMessageUtil.getImmunityTime(checkImmunityTimeStr, transactionTimeout);
        Assert.assertEquals(7 * 1000, immunityTime);


        checkImmunityTimeStr = null;
        immunityTime = TransactionalMessageUtil.getImmunityTime(checkImmunityTimeStr, transactionTimeout);
        Assert.assertEquals(6 * 1000, immunityTime);

        checkImmunityTimeStr = "-1";
        immunityTime = TransactionalMessageUtil.getImmunityTime(checkImmunityTimeStr, transactionTimeout);
        Assert.assertEquals(6 * 1000, immunityTime);

        checkImmunityTimeStr = "60";
        immunityTime = TransactionalMessageUtil.getImmunityTime(checkImmunityTimeStr, transactionTimeout);
        Assert.assertEquals(60 * 1000, immunityTime);

        checkImmunityTimeStr = "100";
        immunityTime = TransactionalMessageUtil.getImmunityTime(checkImmunityTimeStr, transactionTimeout);
        Assert.assertEquals(100 * 1000, immunityTime);


        checkImmunityTimeStr = "100.5";
        immunityTime = TransactionalMessageUtil.getImmunityTime(checkImmunityTimeStr, transactionTimeout);
        Assert.assertEquals(6 * 1000, immunityTime);
    }
}