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
package org.apache.rocketmq.client.exception;

public class MQRedirectException extends MQBrokerException {
    private static final StackTraceElement[] UNASSIGNED_STACK = new StackTraceElement[0];

    private final byte[] body;

    public MQRedirectException(byte[] responseBody) {
        this.body = responseBody;
    }

    // This exception class is used as a flow control item, so stack trace is useless and performance killer.
    @Override
    public synchronized Throwable fillInStackTrace() {
        this.setStackTrace(UNASSIGNED_STACK);
        return this;
    }

    public byte[] getBody() {
        return body;
    }
}
