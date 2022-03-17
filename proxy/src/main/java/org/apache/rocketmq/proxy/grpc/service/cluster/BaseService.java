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
package org.apache.rocketmq.proxy.grpc.service.cluster;

import com.google.rpc.Code;
import io.grpc.Context;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.consumer.ReceiptHandle;
import org.apache.rocketmq.proxy.connector.ConnectorManager;
import org.apache.rocketmq.proxy.grpc.common.ProxyException;

public class BaseService {

    protected final ConnectorManager connectorManager;

    public BaseService(ConnectorManager connectorManager) {
        this.connectorManager = connectorManager;
    }

    protected ReceiptHandle resolveReceiptHandle(Context ctx, String receiptHandleStr) {
        ReceiptHandle receiptHandle = ReceiptHandle.decode(receiptHandleStr);
        if (receiptHandle.isExpired()) {
            throw new ProxyException(Code.INVALID_ARGUMENT, "handle has expired");
        }
        return receiptHandle;
    }

    protected String getBrokerAddr(Context ctx, String brokerName) throws Exception {
        if (StringUtils.isBlank(brokerName)) {
            throw new ProxyException(Code.INVALID_ARGUMENT, "broker name is empty");
        }
        String addr = this.connectorManager.getTopicRouteCache().getBrokerAddr(brokerName);
        if (StringUtils.isBlank(addr)) {
            throw new ProxyException(Code.NOT_FOUND, brokerName + " not exist");
        }
        return addr;
    }
}
