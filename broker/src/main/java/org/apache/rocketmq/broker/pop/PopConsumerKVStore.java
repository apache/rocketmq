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
package org.apache.rocketmq.broker.pop;

import java.util.List;

public interface PopConsumerKVStore {

    /**
     * Starts the storage service.
     */
    boolean start();

    /**
     * Shutdown the storage service.
     */
    boolean shutdown();

    /**
     * Gets the file path of the storage.
     * @return The file path of the storage.
     */
    String getFilePath();

    /**
     * Writes a list of consumer records to the storage.
     * @param consumerRecordList The list of consumer records to be written.
     */
    void writeRecords(List<PopConsumerRecord> consumerRecordList);

    /**
     * Deletes a list of consumer records from the storage.
     * @param consumerRecordList The list of consumer records to be deleted.
     */
    void deleteRecords(List<PopConsumerRecord> consumerRecordList);

    /**
     * Scans and returns a list of expired consumer records within the specified time range.
     * @param lowerTime The start time (inclusive) of the time range to search, in milliseconds.
     * @param upperTime The end time (exclusive) of the time range to search, in milliseconds.
     * @param maxCount The maximum number of records to return.
     *                 Even if more records match the criteria, only this many will be returned.
     * @return A list of expired consumer records within the specified time range.
     *         If no matching records are found, an empty list is returned.
     */
    List<PopConsumerRecord> scanExpiredRecords(long lowerTime, long upperTime, int maxCount);
}
