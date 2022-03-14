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

/**
 * The difference between {@link AuthorisationException} and {@link AuthenticationException} is that
 * {@link AuthenticationException} here means current user's identity could not be recognized.
 *
 * <p>For example, {@link AuthenticationException} will be thrown if access key is invalid.
 */
public class AuthenticationException extends ClientException {
    public AuthenticationException(ErrorCode code, String message, String requestId) {
        super(code, message);
        putMetadata(REQUEST_ID_KEY, requestId);
    }
}
