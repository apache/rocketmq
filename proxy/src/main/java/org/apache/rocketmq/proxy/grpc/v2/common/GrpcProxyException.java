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
import java.util.HashMap;
import java.util.Map;
import org.apache.rocketmq.proxy.common.ProxyException;
import org.apache.rocketmq.proxy.common.ProxyExceptionCode;

public class GrpcProxyException extends RuntimeException {

    private ProxyException proxyException;
    private Code code;

    private static final Map<ProxyExceptionCode, Code> CODE_MAPPING = new HashMap<>();

    static {
        CODE_MAPPING.put(ProxyExceptionCode.INVALID_BROKER_NAME, Code.INVALID_RECEIPT_HANDLE);
        CODE_MAPPING.put(ProxyExceptionCode.RECEIPT_HANDLE_EXPIRED, Code.RECEIPT_HANDLE_EXPIRED);
        CODE_MAPPING.put(ProxyExceptionCode.FORBIDDEN, Code.FORBIDDEN);
    }

    public GrpcProxyException(Code code, String message) {
        super(message);
        this.code = code;
    }

    public GrpcProxyException(Code code, String message, Throwable t) {
        super(message, t);
        this.code = code;
    }

    public GrpcProxyException(ProxyException proxyException) {
        super(proxyException);
        this.proxyException = proxyException;
    }

    public Code getCode() {
        if (this.code != null) {
            return this.code;
        }
        if (this.proxyException != null) {
            return CODE_MAPPING.getOrDefault(this.proxyException.getCode(), Code.INTERNAL_SERVER_ERROR);
        }
        return Code.INTERNAL_SERVER_ERROR;
    }
}
