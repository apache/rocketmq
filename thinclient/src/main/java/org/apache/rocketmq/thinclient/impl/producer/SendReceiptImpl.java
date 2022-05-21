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

package org.apache.rocketmq.thinclient.impl.producer;

import apache.rocketmq.v2.Code;
import apache.rocketmq.v2.SendMessageResponse;
import apache.rocketmq.v2.SendResultEntry;
import apache.rocketmq.v2.Status;
import com.google.common.base.MoreObjects;
import java.util.ArrayList;
import java.util.List;
import org.apache.rocketmq.apis.ClientException;
import org.apache.rocketmq.apis.message.MessageId;
import org.apache.rocketmq.apis.producer.SendReceipt;
import org.apache.rocketmq.thinclient.message.MessageIdCodec;
import org.apache.rocketmq.thinclient.route.Endpoints;
import org.apache.rocketmq.thinclient.route.MessageQueueImpl;

public class SendReceiptImpl implements SendReceipt {
    private final MessageId messageId;
    private final String transactionId;
    private final MessageQueueImpl messageQueue;
    private final long offset;

    private SendReceiptImpl(MessageId messageId, String transactionId, MessageQueueImpl messageQueue, long offset) {
        this.messageId = messageId;
        this.transactionId = transactionId;
        this.messageQueue = messageQueue;
        this.offset = offset;
    }

    @Override
    public MessageId getMessageId() {
        return messageId;
    }

    public MessageQueueImpl getMessageQueue() {
        return messageQueue;
    }

    public String getTransactionId() {
        return transactionId;
    }

    public Endpoints getEndpoints() {
        return messageQueue.getBroker().getEndpoints();
    }

    public long getOffset() {
        return offset;
    }

    public static List<SendReceiptImpl> processSendResponse(MessageQueueImpl mq,
        SendMessageResponse response) throws ClientException {
        final Status status = response.getStatus();
        final Code code = status.getCode();
        if (!Code.OK.equals(code)) {
            throw new ClientException(code.getNumber(), status.getMessage());
        }

        List<SendReceiptImpl> sendReceipts = new ArrayList<>();
        List<Throwable> throwableList = new ArrayList<>();
        final List<SendResultEntry> list = response.getEntriesList();
        for (SendResultEntry entry : list) {
            final Status entryStatus = entry.getStatus();
            final Code statusCode = entryStatus.getCode();
            if (!Code.OK.equals(statusCode)) {
                ClientException e = new ClientException(statusCode.getNumber(), status.getMessage());
                throwableList.add(e);
                continue;
            }
            final SendReceiptImpl impl = new SendReceiptImpl(MessageIdCodec.getInstance().decode(entry.getMessageId()), entry.getTransactionId(), mq, entry.getOffset());
            sendReceipts.add(impl);
        }
        if (!throwableList.isEmpty()) {
            throw new ClientException(throwableList.toArray(new Throwable[0]));
        }
        return sendReceipts;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("messageId", messageId)
            .add("messageQueue", messageQueue)
            .add("offset", offset)
            .toString();
    }
}
