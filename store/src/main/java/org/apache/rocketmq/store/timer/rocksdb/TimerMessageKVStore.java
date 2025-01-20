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
package org.apache.rocketmq.store.timer.rocksdb;

import java.util.List;

public interface TimerMessageKVStore {

    /**
     * Start the timer message kv store.
     */
    boolean start();

    /**
     * Shutdown the timer message kv store.
     */
    boolean shutdown();

    /**
     * Get the file path of the timer message kv store.
     */
    String getFilePath();

    /**
     * Write the timer message records to the timer message kv store.
     * @param columnFamily the column family of the timer message kv store.
     * @param consumerRecordList the list of timer message records to be written.
     * @param offset the cq offset of the timer message records to be written.
     * @param timestamp the key of the timer message metric column family.
     */
    void writeAssignRecords(byte[] columnFamily, List<TimerMessageRecord> consumerRecordList, long offset, int timestamp);

    /**
     * Delete the timer message records from the timer message kv store.
     * @param columnFamily the column family of the timer message kv store.
     * @param consumerRecordList the list of timer message records to be deleted.
     * @param timestamp the key of the timer message metric column family.
     */
    void deleteAssignRecords(byte[] columnFamily, List<TimerMessageRecord> consumerRecordList, int timestamp, long offset);

    /**
     * Scan the timer message records from the timer message kv store.
     * @param columnFamily the column family of the timer message kv store.
     * @param lowerTime the lower time of the timer message records to be scanned.
     * @param upperTime the upper time of the timer message records to be scanned.
     * @return the list of timer message records.
     */
    List<TimerMessageRecord> scanRecords(byte[] columnFamily, long lowerTime, long upperTime, long timestamp);

    /**
     * Get the commit offset of the timer message kv store from cq.
     * @return the commit offset of the timer message kv store.
     */
    long getCommitOffset();

    /**
     * Get the metric size of the timer message kv store.
     * @param lowerTime the lower time of the timer message records to be scanned.
     * @param upperTime the upper time of the timer message records to be scanned.
     * @return sum.
     */
    int getMetricSize(int lowerTime, int upperTime);

    /**
     * Get the checkpoint of the timer message kv store.
     * @param columnFamily the column family of the timer message kv store.
     * @return the checkpoint of the timer message kv store.
     */
    long getCheckpoint(byte[] columnFamily);
}
