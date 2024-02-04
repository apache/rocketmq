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

import org.apache.rocketmq.common.consumer.ReceiptHandle;
import org.apache.rocketmq.common.utils.AbstractStartAndShutdown;
import org.apache.rocketmq.proxy.common.ProxyException;
import org.apache.rocketmq.proxy.common.ProxyExceptionCode;
import org.apache.rocketmq.proxy.service.ServiceManager;

public abstract class AbstractProcessor extends AbstractStartAndShutdown {

    protected MessagingProcessor messagingProcessor;
    protected ServiceManager serviceManager;

    public AbstractProcessor(MessagingProcessor messagingProcessor,
        ServiceManager serviceManager) {
        this.messagingProcessor = messagingProcessor;
        this.serviceManager = serviceManager;
    }

    protected void validateReceiptHandle(ReceiptHandle handle) {
        if (handle.isExpired()) {
            throw new ProxyException(ProxyExceptionCode.INVALID_RECEIPT_HANDLE, "receipt handle is expired");
        }
    }
}
