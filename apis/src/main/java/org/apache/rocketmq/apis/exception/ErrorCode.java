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
 * Indicates the reason why the exception is thrown, it can be easily divided into the following categories.
 *
 * <blockquote>
 *
 * <table>
 * <caption>Error Categories and Exceptions</caption>
 * <tr>
 *     <th>Category
 *     <th>Exception
 *     <th>Code range
 * <tr>
 *     <td>Illegal client argument
 *     <td>{@link RemoteIllegalArgumentException}
 *     <p>{@link IllegalArgumentException}
 *     <td>{@code [101..199]}
 * <tr>
 *     <td>Authorisation failure
 *     <td>{@link AuthorisationException}
 *     <td>{@code [201..299]}
 * <tr>
 *     <td>Resource not found
 *     <td>{@link ResourceNotFoundException}
 *     <td>{@code [301..399]}
 * </table>
 *
 * </blockquote>
 */
public enum ErrorCode {
    /**
     * Format of topic is illegal.
     */
    INVALID_TOPIC(101),
    /**
     * Format of consumer group is illegal.
     */
    INVALID_CONSUMER_GROUP(102),
    /**
     * Message is forbidden to publish.
     */
    MESSAGE_PUBLISH_FORBIDDEN(201),
    /**
     * Topic does not exist.
     */
    TOPIC_DOES_NOT_EXIST(301),
    /**
     * Consumer group does not exist.
     */
    CONSUMER_GROUP_DOES_NOT_EXIST(302);

    private final int code;

    ErrorCode(int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }

    @Override
    public String toString() {
        return String.valueOf(code);
    }
}
