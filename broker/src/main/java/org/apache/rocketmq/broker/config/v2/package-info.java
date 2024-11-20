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
package org.apache.rocketmq.broker.config.v2;

/*
 * <strong>Endian</strong>: we use network byte order for all integrals, aka, always big endian.
 *
 * Unlike v1 config managers, implementations in this package prioritize data integrity and reliability.
 * As a result,RocksDB write-ahead-log is always on and changes are immediately flushed. Another significant
 * difference is that heap-based cache is removed because it is not necessary and duplicated to RocksDB
 * MemTable/BlockCache.
 */
