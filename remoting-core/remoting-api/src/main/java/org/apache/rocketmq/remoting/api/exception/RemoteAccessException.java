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
 * Generic remote access exception. A service proxy for any remoting
 * protocol should throw this exception or subclasses of it, in order
 * to transparently expose a plain Java business interface.
 *
 * <p>A client may catch RemoteAccessException if it wants to, but as
 * remote access errors are typically unrecoverable, it will probably let
 * such exceptions propagate to a higher level that handles them generically.
 * In this case, the client opCode doesn't show any signs of being involved in
 * remote access, as there aren't any remoting-specific dependencies.
 *
 * @since 1.0.0
 */
public class RemoteAccessException extends NestedRuntimeException {
    private static final long serialVersionUID = 6280428909532427263L;

    /**
     * Constructor for RemoteAccessException with the specified detail message.
     *
     * @param msg the detail message
     */
    public RemoteAccessException(String msg) {
        super(msg);
    }

    /**
     * Constructor for RemoteAccessException with the specified detail message
     * and nested exception.
     *
     * @param msg the detail message
     * @param cause the root cause (usually from using an underlying
     * remoting API such as RMI)
     */
    public RemoteAccessException(String msg, Throwable cause) {
        super(msg, cause);
    }

}
