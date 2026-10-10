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

import com.google.protobuf.Descriptors;
import com.google.protobuf.MessageOrBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

public class ResponseWriter {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.PROXY_LOGGER_NAME);

    protected static final Object INSTANCE_CREATE_LOCK = new Object();
    protected static volatile ResponseWriter instance;

    public static ResponseWriter getInstance() {
        if (instance == null) {
            synchronized (INSTANCE_CREATE_LOCK) {
                if (instance == null) {
                    instance = new ResponseWriter();
                }
            }
        }
        return instance;
    }

    public <T> void write(StreamObserver<T> observer, final T response) {
        if (writeResponse(observer, response)) {
            observer.onCompleted();
        }
    }

    public <T> boolean writeResponse(StreamObserver<T> observer, final T response) {
        if (null == response) {
            return false;
        }
        if (log.isDebugEnabled()) {
            log.debug("start to write response. responseSummary: {}", summarizeResponse(response));
        }
        if (isCancelled(observer)) {
            log.warn("client has cancelled the request. responseSummary: {}", summarizeResponse(response));
            return false;
        }
        try {
            observer.onNext(response);
        } catch (StatusRuntimeException statusRuntimeException) {
            if (Status.CANCELLED.equals(statusRuntimeException.getStatus())) {
                log.warn("client has cancelled the request. responseSummary: {}", summarizeResponse(response));
                return false;
            }
            throw statusRuntimeException;
        }
        return true;
    }

    static String summarizeResponse(Object response) {
        if (!(response instanceof MessageOrBuilder)) {
            return response.getClass().getSimpleName();
        }
        MessageOrBuilder message = (MessageOrBuilder) response;
        StringBuilder summary = new StringBuilder()
            .append("type=")
            .append(message.getDescriptorForType().getFullName());
        appendStatusCode(summary, message);
        return summary.toString();
    }

    private static void appendStatusCode(StringBuilder summary, MessageOrBuilder message) {
        Descriptors.FieldDescriptor statusField = message.getDescriptorForType().findFieldByName("status");
        if (statusField == null || statusField.isRepeated()
            || statusField.getJavaType() != Descriptors.FieldDescriptor.JavaType.MESSAGE
            || !message.hasField(statusField)) {
            return;
        }
        Object status = message.getField(statusField);
        if (!(status instanceof MessageOrBuilder)) {
            return;
        }
        MessageOrBuilder statusMessage = (MessageOrBuilder) status;
        Descriptors.FieldDescriptor codeField = statusMessage.getDescriptorForType().findFieldByName("code");
        if (codeField == null || codeField.isRepeated()
            || codeField.getJavaType() != Descriptors.FieldDescriptor.JavaType.ENUM) {
            return;
        }
        Object code = statusMessage.getField(codeField);
        if (code instanceof Descriptors.EnumValueDescriptor) {
            summary.append(", statusCode=").append(((Descriptors.EnumValueDescriptor) code).getName());
        }
    }

    public <T> boolean isCancelled(StreamObserver<T> observer) {
        if (observer instanceof ServerCallStreamObserver) {
            final ServerCallStreamObserver<T> serverCallStreamObserver = (ServerCallStreamObserver<T>) observer;
            return serverCallStreamObserver.isCancelled();
        }
        return false;
    }
}
