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

/**
 * See <a href="https://book.tidb.io/session1/chapter3/tidb-kv-to-relation.html">Table, Key Value Mapping</a>
 */
public enum TableId {
    UNSPECIFIED((short) 0),
    CONSUMER_OFFSET((short) 1),
    PULL_OFFSET((short) 2),
    TOPIC((short) 3),
    SUBSCRIPTION_GROUP((short) 4);

    private final short value;

    TableId(short value) {
        this.value = value;
    }

    public short getValue() {
        return value;
    }
}
