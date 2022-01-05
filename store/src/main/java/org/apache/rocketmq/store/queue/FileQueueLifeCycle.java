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
package org.apache.rocketmq.store.queue;

import org.apache.rocketmq.store.DispatchRequest;
import org.apache.rocketmq.store.MessageExtBrokerInner;
import org.apache.rocketmq.store.Swappable;

public interface FileQueueLifeCycle extends Swappable {
    boolean load();
    void recover();
    void checkSelf();
    boolean flush(int flushLeastPages);
    void destroy();
    void truncateDirtyLogicFiles(long maxCommitLogPos);
    int deleteExpiredFile(long minCommitLogPos);
    long rollNextFile(final long offset);
    boolean isFirstFileAvailable();
    boolean isFirstFileExist();
    void correctMinOffset(long minCommitLogOffset);
    void putMessagePositionInfoWrapper(DispatchRequest request);
    void assignQueueOffset(QueueOffsetAssigner queueOffsetAssigner, MessageExtBrokerInner msg, short messageNum);
}
