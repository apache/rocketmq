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
package org.apache.rocketmq.common.constant;

public class TopicMessageTypeName {
    public static final int INDEX_TRANSACTION = 4;
    public static final int INDEX_DELAY = 3;
    public static final int INDEX_FIFO = 2;
    public static final int INDEX_NORMAL = 1;

    public static final int TRANSACTION = 0x1 << INDEX_TRANSACTION;
    public static final int DELAY = 0x1 << INDEX_DELAY;
    public static final int FIFO = 0x1 << INDEX_FIFO;
    public static final int NORMAL = 0x1 << INDEX_NORMAL;
    public static final int UNSPECIFIED = 0;

    public static final int ALL = NORMAL | FIFO | DELAY | TRANSACTION;

    public static boolean isUnspecified(final int type) {
        return type == UNSPECIFIED;
    }

    public static boolean isNormal(final int type) {
        return (type & NORMAL) == NORMAL;
    }

    public static boolean isFifo(final int type) {
        return (type & FIFO) == FIFO;
    }

    public static boolean isDelay(final int type) {
        return (type & DELAY) == DELAY;
    }

    public static boolean isTransaction(final int type) {
        return (type & TRANSACTION) == TRANSACTION;
    }
}
