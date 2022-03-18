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

package org.apache.rocketmq.apis.exception;

import org.apache.rocketmq.apis.message.Message;
import org.apache.rocketmq.apis.producer.Producer;

/**
 * The difference between {@link AuthenticationException} and {@link AuthorisationException} is that
 * {@link AuthorisationException} here means current users don't have permission to do current operation.
 *
 * <p>For example, current user is forbidden to send message to this topic, {@link AuthorisationException} will be
 * thrown in {@link Producer#send(Message)}.
 */
public class AuthorisationException extends ClientException {
    public AuthorisationException(ErrorCode code, String message, String requestId) {
        super(code, message);
        putMetadata(REQUEST_ID_KEY, requestId);
    }
}
