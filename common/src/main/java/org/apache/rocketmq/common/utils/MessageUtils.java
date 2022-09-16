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
package org.apache.rocketmq.common.utils;

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import com.google.common.hash.Hashing;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;

public class MessageUtils {

    public static int getShardingKeyIndex(String shardingKey, int indexSize) {
        return Math.abs(Hashing.murmur3_32().hashBytes(shardingKey.getBytes(StandardCharsets.UTF_8)).asInt() % indexSize);
    }

    public static int getShardingKeyIndexByMsg(MessageExt msg, int indexSize) {
        String shardingKey = msg.getProperty(MessageConst.PROPERTY_SHARDING_KEY);
        if (shardingKey == null) {
            shardingKey = "";
        }

        return getShardingKeyIndex(shardingKey, indexSize);
    }

    public static Set<Integer> getShardingKeyIndexes(Collection<MessageExt> msgs, int indexSize) {
        Set<Integer> indexSet = new HashSet<Integer>(indexSize);
        for (MessageExt msg : msgs) {
            indexSet.add(getShardingKeyIndexByMsg(msg, indexSize));
        }
        return indexSet;
    }
}
