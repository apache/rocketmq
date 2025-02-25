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

/**
 * $Id: SendMessageResponseHeader.java 1835 2013-05-16 02:00:50Z vintagewang@apache.org $
 */
package org.apache.rocketmq.remoting.protocol.header;

import io.netty.buffer.ByteBuf;
import java.util.HashMap;
import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.annotation.CFNotNull;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.protocol.FastCodesHeader;

public class SendMessageResponseHeader implements CommandCustomHeader, FastCodesHeader {
    @CFNotNull
    private String msgId;
    @CFNotNull
    private Integer queueId;
    @CFNotNull
    private Long queueOffset;
    private String transactionId;
    private String batchUniqId;
    private String recallHandle;

    @Override
    public void checkFields() throws RemotingCommandException {
    }

    @Override
    public void encode(ByteBuf out) {
        writeIfNotNull(out, "msgId", msgId);
        writeIfNotNull(out, "queueId", queueId);
        writeIfNotNull(out, "queueOffset", queueOffset);
        writeIfNotNull(out, "transactionId", transactionId);
        writeIfNotNull(out, "batchUniqId", batchUniqId);
        writeIfNotNull(out, "recallHandle", recallHandle);
    }

    @Override
    public void decode(HashMap<String, String> fields) throws RemotingCommandException {
        String str = getAndCheckNotNull(fields, "msgId");
        if (str != null) {
            this.msgId = str;
        }

        str = getAndCheckNotNull(fields, "queueId");
        if (str != null) {
            this.queueId = Integer.parseInt(str);
        }

        str = getAndCheckNotNull(fields, "queueOffset");
        if (str != null) {
            this.queueOffset = Long.parseLong(str);
        }

        str = fields.get("transactionId");
        if (str != null) {
            this.transactionId = str;
        }

        str = fields.get("batchUniqId");
        if (str != null) {
            this.batchUniqId = str;
        }

        str = fields.get("recallHandle");
        if (str != null) {
            this.recallHandle = str;
        }
    }

    public String getMsgId() {
        return msgId;
    }

    public void setMsgId(String msgId) {
        this.msgId = msgId;
    }

    public Integer getQueueId() {
        return queueId;
    }

    public void setQueueId(Integer queueId) {
        this.queueId = queueId;
    }

    public Long getQueueOffset() {
        return queueOffset;
    }

    public void setQueueOffset(Long queueOffset) {
        this.queueOffset = queueOffset;
    }

    public String getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(String transactionId) {
        this.transactionId = transactionId;
    }

    public String getBatchUniqId() {
        return batchUniqId;
    }

    public void setBatchUniqId(String batchUniqId) {
        this.batchUniqId = batchUniqId;
    }

    public String getRecallHandle() {
        return recallHandle;
    }

    public void setRecallHandle(String recallHandle) {
        this.recallHandle = recallHandle;
    }
}
