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
package org.apache.rocketmq.store;

import org.rocksdb.RocksDBException;

/**
 * Interface for stores that require commitlog dispatch and recovery. Each store implementing this interface should
 * register itself in the commitlog when loading. This abstraction allows the commitlog recovery process to
 * automatically consider all registered stores without needing to modify the recovery logic when adding a new store.
 */
public interface CommitLogDispatchStore {

    /**
     * Get the dispatch offset in the store. Messages whose phyOffset larger than this offset need to be dispatched. The
     * dispatch offset is only used during recovery.
     *
     * @param recoverNormally true if broker exited normally last time (normal recovery), false for abnormal recovery
     * @return the dispatch phyOffset, or null if the store is not enabled or has no valid offset
     * @throws RocksDBException if there is an error accessing RocksDB storage
     */
    Long getDispatchFromPhyOffset(boolean recoverNormally) throws RocksDBException;

    /**
     * Used to determine whether to start doDispatch from this commitLog mappedFile.
     *
     * @param phyOffset the offset of the first message in this commitlog mappedFile
     * @param storeTimestamp the timestamp of the first message in this commitlog mappedFile
     * @param recoverNormally whether this is a normal recovery
     * @return whether to start recovering from this MappedFile
     * @throws RocksDBException if there is an error accessing RocksDB storage
     */
    boolean isMappedFileMatchedRecover(long phyOffset, long storeTimestamp,
        boolean recoverNormally) throws RocksDBException;
}

