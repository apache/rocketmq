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
package org.apache.rocketmq.tieredstore.exception;

public enum TieredStoreErrorCode {

    /**
     * Error code for an invalid offset.
     */
    ILLEGAL_OFFSET,

    /**
     * Error code for an invalid parameter.
     */
    ILLEGAL_PARAM,

    /**
     * Error code for an incorrect download length.
     */
    DOWNLOAD_LENGTH_NOT_CORRECT,

    /**
     * Error code for no new data found in the storage system.
     */
    NO_NEW_DATA,

    /**
     * Error code for a storage provider error.
     */
    STORAGE_PROVIDER_ERROR,

    /**
     * Error code for an input/output error.
     */
    IO_ERROR,

    /**
     * Segment has been sealed
     */
    SEGMENT_SEALED,

    /**
     * Error code for an unknown error.
     */
    UNKNOWN
}