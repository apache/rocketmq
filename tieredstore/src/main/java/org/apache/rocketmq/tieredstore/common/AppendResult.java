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
package org.apache.rocketmq.tieredstore.common;

public enum AppendResult {

    /**
     * The append operation was successful.
     */
    SUCCESS,

    /**
     * The offset provided for the append operation is incorrect.
     */
    OFFSET_INCORRECT,

    /**
     * The buffer used for the append operation is full.
     */
    BUFFER_FULL,

    /**
     * The file is full and cannot accept more data.
     */
    FILE_FULL,

    /**
     * There was an I/O error during the append operation.
     */
    IO_ERROR,

    /**
     * The file is closed and cannot accept more data.
     */
    FILE_CLOSED,

    /**
     * An unknown error occurred during the append operation.
     */
    UNKNOWN_ERROR
}
