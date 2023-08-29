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

package org.apache.rocketmq.proxy.processor;

import org.apache.rocketmq.client.consumer.AckResult;
import org.apache.rocketmq.proxy.common.ProxyException;
import org.apache.rocketmq.proxy.service.message.ReceiptHandleMessage;

public class BatchAckResult {

    private final ReceiptHandleMessage receiptHandleMessage;
    private AckResult ackResult;
    private ProxyException proxyException;

    public BatchAckResult(ReceiptHandleMessage receiptHandleMessage,
        AckResult ackResult) {
        this.receiptHandleMessage = receiptHandleMessage;
        this.ackResult = ackResult;
    }

    public BatchAckResult(ReceiptHandleMessage receiptHandleMessage,
        ProxyException proxyException) {
        this.receiptHandleMessage = receiptHandleMessage;
        this.proxyException = proxyException;
    }

    public ReceiptHandleMessage getReceiptHandleMessage() {
        return receiptHandleMessage;
    }

    public AckResult getAckResult() {
        return ackResult;
    }

    public ProxyException getProxyException() {
        return proxyException;
    }
}
