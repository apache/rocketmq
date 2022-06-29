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

package org.apache.rocketmq.proxy.grpc.v2.common;

import apache.rocketmq.v2.Code;
import apache.rocketmq.v2.Status;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.rocketmq.client.common.ClientErrorCode;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.proxy.common.ProxyException;
import org.apache.rocketmq.proxy.common.utils.ExceptionUtils;
import org.apache.rocketmq.proxy.service.route.TopicRouteHelper;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;

public class ResponseBuilder {

    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);
    public static final Map<Integer, Code> RESPONSE_CODE_MAPPING = new ConcurrentHashMap<>();

    static {
        RESPONSE_CODE_MAPPING.put(ResponseCode.SUCCESS, Code.OK);
        RESPONSE_CODE_MAPPING.put(ResponseCode.SYSTEM_BUSY, Code.TOO_MANY_REQUESTS);
        RESPONSE_CODE_MAPPING.put(ResponseCode.REQUEST_CODE_NOT_SUPPORTED, Code.NOT_IMPLEMENTED);
        RESPONSE_CODE_MAPPING.put(ResponseCode.SUBSCRIPTION_GROUP_NOT_EXIST, Code.CONSUMER_GROUP_NOT_FOUND);
        RESPONSE_CODE_MAPPING.put(ClientErrorCode.ACCESS_BROKER_TIMEOUT, Code.PROXY_TIMEOUT);
    }

    public static Status buildStatus(Throwable t) {
        t = ExceptionUtils.getRealException(t);

        if (t instanceof ProxyException) {
            t = new GrpcProxyException((ProxyException) t);
        }
        if (t instanceof GrpcProxyException) {
            GrpcProxyException grpcProxyException = (GrpcProxyException) t;
            return ResponseBuilder.buildStatus(grpcProxyException.getCode(), grpcProxyException.getMessage());
        }
        if (TopicRouteHelper.isTopicNotExistError(t)) {
            return ResponseBuilder.buildStatus(Code.TOPIC_NOT_FOUND, t.getMessage());
        }
        if (t instanceof MQBrokerException) {
            MQBrokerException mqBrokerException = (MQBrokerException) t;
            return ResponseBuilder.buildStatus(buildCode(mqBrokerException.getResponseCode()), mqBrokerException.getErrorMessage());
        }
        if (t instanceof MQClientException) {
            MQClientException mqClientException = (MQClientException) t;
            return ResponseBuilder.buildStatus(buildCode(mqClientException.getResponseCode()), mqClientException.getErrorMessage());
        }
        if (t instanceof RemotingTimeoutException) {
            return ResponseBuilder.buildStatus(Code.PROXY_TIMEOUT, t.getMessage());
        }

        log.error("internal server error", t);
        return ResponseBuilder.buildStatus(Code.INTERNAL_SERVER_ERROR, ExceptionUtils.getErrorDetailMessage(t));
    }

    public static Status buildStatus(Code code, String message) {
        return Status.newBuilder()
            .setCode(code)
            .setMessage(message)
            .build();
    }

    public static Status buildStatus(int remotingResponseCode, String remark) {
        String message = remark;
        if (message == null) {
            message = String.valueOf(remotingResponseCode);
        }
        return Status.newBuilder()
            .setCode(buildCode(remotingResponseCode))
            .setMessage(message)
            .build();
    }

    public static Code buildCode(int remotingResponseCode) {
        return RESPONSE_CODE_MAPPING.getOrDefault(remotingResponseCode, Code.INTERNAL_SERVER_ERROR);
    }
}
