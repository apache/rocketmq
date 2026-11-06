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
package org.apache.rocketmq.proxy.common.utils;

import org.apache.rocketmq.common.consumer.ReceiptHandle;
import org.apache.rocketmq.proxy.common.ProxyException;
import org.apache.rocketmq.proxy.common.ProxyExceptionCode;

public class ProxyUtils {

    public static final int MAX_MSG_NUMS_FOR_POP_REQUEST = 32;

    public static final String BROKER_ADDR = "brokerAddr";

    /**
     * Decodes a receipt handle, reporting one that cannot be parsed as
     * {@link ProxyExceptionCode#INVALID_RECEIPT_HANDLE} instead of letting the parse error surface
     * as an internal server error.
     *
     * <p>This mirrors how an expired handle is reported ({@code AbstractProcessor#validateReceiptHandle}),
     * and it matters to the official clients: they only give up on a message when the status is
     * {@code INVALID_RECEIPT_HANDLE} and retry the ack forever on any other non-OK code.
     */
    public static ReceiptHandle decodeReceiptHandle(String receiptHandle) {
        try {
            return ReceiptHandle.decode(receiptHandle);
        } catch (RuntimeException e) {
            throw new ProxyException(ProxyExceptionCode.INVALID_RECEIPT_HANDLE,
                "invalid receipt handle: " + e.getMessage());
        }
    }
}
