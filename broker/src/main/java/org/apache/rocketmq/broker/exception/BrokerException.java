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

package org.apache.rocketmq.broker.exception;

/**
 * Broker exception.
 */
public class BrokerException extends Exception {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Creates a new exception with an error message.
     *
     * @param msg Error message.
     */
    public BrokerException(String msg) {
        super(msg);
    }

    /**
     * Creates a new exception with an error message and throwable as a cause.
     *
     * @param cause Throwable cause.
     */
    public BrokerException(Throwable cause) {
        super(cause);
    }

    /**
     * Creates a new exception with an error message and throwable as a cause.
     *
     * @param msg Error message.
     * @param cause Throwable cause.
     */
    public BrokerException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
