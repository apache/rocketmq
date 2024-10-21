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

package org.apache.rocketmq.store.queue.offset;

public class OffsetEntry {
    /**
     * Topic identifier. For now, it's topic name directly. In the future, we should use fixed length topic identifier.
     */
    public String topic;

    /**
     * Queue ID
     */
    public int queueId;

    /**
     * Flag if the entry is for maximum or minimum
     */
    public OffsetEntryType type;

    /**
     * Maximum or minimum consume-queue offset.
     */
    public long offset;

    /**
     * Maximum or minimum commit-log offset.
     */
    public long commitLogOffset;
}
