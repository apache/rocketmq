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

package org.apache.rocketmq.proxy.service.message;

import org.apache.rocketmq.common.consumer.ReceiptHandle;

public class ReceiptHandleMessage {

    private final ReceiptHandle receiptHandle;
    private final String messageId;
    private final String liteTopic;
    private final long invisibleTime;

    public ReceiptHandleMessage(ReceiptHandle receiptHandle, String messageId) {
        this(receiptHandle, messageId, null, -1);
    }

    public ReceiptHandleMessage(ReceiptHandle receiptHandle, String messageId, String liteTopic) {
        this(receiptHandle, messageId, liteTopic, -1);
    }

    public ReceiptHandleMessage(ReceiptHandle receiptHandle, String messageId, String liteTopic, long invisibleTime) {
        this.receiptHandle = receiptHandle;
        this.messageId = messageId;
        this.liteTopic = liteTopic;
        this.invisibleTime = invisibleTime;
    }

    public ReceiptHandle getReceiptHandle() {
        return receiptHandle;
    }

    public String getMessageId() {
        return messageId;
    }

    public String getLiteTopic() {
        return liteTopic;
    }

    public long getInvisibleTime() {
        return invisibleTime;
    }
}
