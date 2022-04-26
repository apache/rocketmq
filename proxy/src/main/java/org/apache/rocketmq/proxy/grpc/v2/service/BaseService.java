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
package org.apache.rocketmq.proxy.grpc.v2.service;

import apache.rocketmq.v2.Code;
import apache.rocketmq.v2.FilterExpression;
import apache.rocketmq.v2.Resource;
import io.grpc.Context;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.consumer.ReceiptHandle;
import org.apache.rocketmq.proxy.common.StartAndShutdown;
import org.apache.rocketmq.proxy.connector.ConnectorManager;
import org.apache.rocketmq.proxy.connector.route.TopicRouteCache;
import org.apache.rocketmq.proxy.grpc.v2.adapter.GrpcConverter;
import org.apache.rocketmq.proxy.grpc.v2.adapter.ProxyException;

public abstract class BaseService implements StartAndShutdown {

    protected final ConnectorManager connectorManager;

    public BaseService(ConnectorManager connectorManager) {
        this.connectorManager = connectorManager;
    }

    public static ReceiptHandle resolveReceiptHandle(Context ctx, String receiptHandleStr) {
        ReceiptHandle receiptHandle = ReceiptHandle.decode(receiptHandleStr);
        if (receiptHandle.isExpired()) {
            throw new ProxyException(Code.RECEIPT_HANDLE_EXPIRED, "handle has expired");
        }
        return receiptHandle;
    }

    public static String getBrokerAddr(Context ctx, TopicRouteCache topicRouteCache, String brokerName) throws Exception {
        if (StringUtils.isBlank(brokerName)) {
            throw new ProxyException(Code.UNRECOGNIZED, "broker name is empty");
        }
        String addr = topicRouteCache.getBrokerAddr(brokerName);
        if (StringUtils.isBlank(addr)) {
            throw new ProxyException(Code.UNRECOGNIZED, brokerName + " not exist");
        }
        return addr;
    }

    protected String getBrokerAddr(Context ctx, String brokerName) throws Exception {
        return getBrokerAddr(ctx, this.connectorManager.getTopicRouteCache(), brokerName);
    }

    protected void checkSubscriptionData(Resource topic, FilterExpression filterExpression) {
        // for checking filterExpression.
        String topicName = GrpcConverter.wrapResourceWithNamespace(topic);
        GrpcConverter.buildSubscriptionData(topicName, filterExpression);
    }

    @Override
    public void start() throws Exception {

    }

    @Override
    public void shutdown() throws Exception {

    }
}
