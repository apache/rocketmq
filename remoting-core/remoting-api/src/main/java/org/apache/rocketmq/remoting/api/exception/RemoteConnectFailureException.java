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

package org.apache.rocketmq.remoting.api.exception;

/**
 * RemoteConnectFailureException will be thrown when connection
 * could not be established with a remote service.
 *
 * @since 1.0.0
 */
public class RemoteConnectFailureException extends RemoteAccessException {
    private static final long serialVersionUID = -5565366231695911316L;

    /**
     * Constructor for RemoteConnectFailureException with the specified detail message
     * and nested exception.
     *
     * @param msg the detail message
     * @param cause the root cause from the remoting API in use
     */
    public RemoteConnectFailureException(String msg, Throwable cause) {
        super(msg, cause);
    }

    /**
     * Constructor for RemoteConnectFailureException with the specified detail message.
     *
     * @param msg the detail message
     */
    public RemoteConnectFailureException(String msg) {
        super(msg);
    }

}
