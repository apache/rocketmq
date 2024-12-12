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

import static org.apache.rocketmq.common.message.MessageDecoder.NAME_VALUE_SEPARATOR;
import static org.apache.rocketmq.common.message.MessageDecoder.PROPERTY_SEPARATOR;

public class MessageUtils {
    private MessageUtils() {
        // Prevent class from being instantiated from outside
    }

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
        Set<Integer> indexSet = new HashSet<>(indexSize);
        for (MessageExt msg : msgs) {
            indexSet.add(getShardingKeyIndexByMsg(msg, indexSize));
        }
        return indexSet;
    }

    public static String deleteProperty(String propertiesString, String name) {
        if (propertiesString != null) {
            int idx0 = 0;
            int idx1;
            int idx2;
            idx1 = propertiesString.indexOf(name, idx0);
            if (idx1 != -1) {
                // cropping may be required
                StringBuilder stringBuilder = new StringBuilder(propertiesString.length());
                while (true) {
                    int startIdx = idx0;
                    while (true) {
                        idx1 = propertiesString.indexOf(name, startIdx);
                        if (idx1 == -1) {
                            break;
                        }
                        startIdx = idx1 + name.length();
                        if ((idx1 == 0 || propertiesString.charAt(idx1 - 1) == PROPERTY_SEPARATOR)
                                && propertiesString.length() > idx1 + name.length()
                                && propertiesString.charAt(idx1 + name.length()) == NAME_VALUE_SEPARATOR) {
                            break;
                        }
                    }
                    if (idx1 == -1) {
                        // there are no characters that need to be skipped. Append all remaining characters.
                        stringBuilder.append(propertiesString, idx0, propertiesString.length());
                        break;
                    }
                    // there are characters that need to be cropped
                    stringBuilder.append(propertiesString, idx0, idx1);
                    // move idx2 to the end of the cropped character
                    idx2 = propertiesString.indexOf(PROPERTY_SEPARATOR, idx1 + name.length() + 1);
                    // all subsequent characters will be cropped
                    if (idx2 == -1) {
                        break;
                    }
                    idx0 = idx2 + 1;
                }
                return stringBuilder.toString();
            }
        }
        return propertiesString;
    }
}
