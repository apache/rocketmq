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
 * Handy class for wrapping runtime {@code Exceptions} with a root cause.
 *
 * <p>This class is {@code abstract} to force the programmer to extend
 * the class. {@code getMessage} will include nested exception
 * information; {@code getRootCause} will include the innermost cause of
 * this exception, if any; {@code printStackTrace} and other like methods will
 * delegate to the wrapped exception, if any.
 *
 * @since 1.0.0
 */
public abstract class NestedRuntimeException extends RuntimeException {
    private static final long serialVersionUID = -8371779880133933367L;

    /**
     * Construct a {@code NestedRuntimeException} with the specified detail message.
     *
     * @param msg the detail message
     */
    public NestedRuntimeException(String msg) {
        super(msg);
    }

    /**
     * Construct a {@code NestedRuntimeException} with the specified detail message
     * and nested exception.
     *
     * @param msg the detail message
     * @param cause the nested exception
     */
    public NestedRuntimeException(String msg, Throwable cause) {
        super(msg, cause);
    }

    /**
     * Build a message for the given base message and root cause.
     *
     * @param message the base message
     * @param cause the root cause
     * @return the full exception message
     */
    private static String getMessageWithCause(String message, Throwable cause) {
        if (cause != null) {
            StringBuilder sb = new StringBuilder();
            if (message != null) {
                sb.append(message).append("; ");
            }
            sb.append("nested exception is ").append(cause);
            return sb.toString();
        } else {
            return message;
        }
    }

    /**
     * Return the detail message, including the message from the nested exception
     * if there is one.
     */
    @Override
    public String getMessage() {
        return getMessageWithCause(super.getMessage(), getCause());
    }

    /**
     * Retrieve the innermost cause of this exception, if any.
     *
     * @return the innermost exception, or {@code null} if none
     */
    public Throwable getRootCause() {
        Throwable rootCause = null;
        Throwable cause = getCause();
        while (cause != null && cause != rootCause) {
            rootCause = cause;
            cause = cause.getCause();
        }
        return rootCause;
    }

}
