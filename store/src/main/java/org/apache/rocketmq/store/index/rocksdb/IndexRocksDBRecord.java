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
package org.apache.rocketmq.store.index.rocksdb;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.message.MessageConst;

public class IndexRocksDBRecord {
    public static final String KEY_SPLIT = "@";
    public static final byte[] KEY_SPLIT_BYTES = KEY_SPLIT.getBytes(StandardCharsets.UTF_8);
    private static final int VALUE_LENGTH = Long.BYTES;
    private long storeTime;
    private String topic;
    private String key;
    private String tag;
    private String uniqKey;
    private long offsetPy;

    public IndexRocksDBRecord(String topic, String key, String tag, long storeTime, String uniqKey, long offsetPy) {
        this.topic = topic;
        this.key = key;
        this.tag = tag;
        this.storeTime = storeTime;
        this.uniqKey = uniqKey;
        this.offsetPy = offsetPy;
    }

    public byte[] getKeyBytes() {
        if (StringUtils.isEmpty(topic) || StringUtils.isEmpty(uniqKey) || offsetPy < 0L || storeTime <= 0L) {
            return null;
        }
        long storeTimeHour = MixAll.dealTimeToHourStamps(storeTime);
        if (storeTimeHour <= 0L) {
            return null;
        }
        String keyMiddleStr;
        if (!StringUtils.isEmpty(key)) {
            keyMiddleStr = KEY_SPLIT + topic + KEY_SPLIT + MessageConst.INDEX_KEY_TYPE + KEY_SPLIT + key + KEY_SPLIT + uniqKey + KEY_SPLIT;
        } else if (!StringUtils.isEmpty(tag)) {
            keyMiddleStr = KEY_SPLIT + topic + KEY_SPLIT + MessageConst.INDEX_TAG_TYPE + KEY_SPLIT + tag + KEY_SPLIT + uniqKey + KEY_SPLIT;
        } else {
            keyMiddleStr = KEY_SPLIT + topic + KEY_SPLIT + MessageConst.INDEX_UNIQUE_TYPE + KEY_SPLIT + uniqKey + KEY_SPLIT;
        }
        if (StringUtils.isEmpty(keyMiddleStr)) {
            return null;
        }
        byte[] keyMiddleBytes = keyMiddleStr.getBytes(StandardCharsets.UTF_8);
        int keyLength = Long.BYTES + keyMiddleBytes.length + Long.BYTES;
        return ByteBuffer.allocate(keyLength).putLong(storeTimeHour).put(keyMiddleBytes).putLong(offsetPy).array();
    }

    public byte[] getValueBytes() {
        if (storeTime <= 0L) {
            return null;
        }
        return ByteBuffer.allocate(VALUE_LENGTH).putLong(storeTime).array();
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public long getStoreTime() {
        return storeTime;
    }

    public void setStoreTime(long storeTime) {
        this.storeTime = storeTime;
    }

    public String getUniqKey() {
        return uniqKey;
    }

    public void setUniqKey(String uniqKey) {
        this.uniqKey = uniqKey;
    }

    public long getOffsetPy() {
        return offsetPy;
    }

    public void setOffsetPy(long offsetPy) {
        this.offsetPy = offsetPy;
    }

    public String getTag() {
        return tag;
    }

    public void setTag(String tag) {
        this.tag = tag;
    }
}
