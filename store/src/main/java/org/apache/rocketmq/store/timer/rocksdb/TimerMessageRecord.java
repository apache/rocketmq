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

import com.alibaba.fastjson.annotation.JSONField;

import java.nio.ByteBuffer;

public class TimerMessageRecord {
    // key: delayTime + offsetPY
    private long delayTime;
    private long offsetPY;

    // value: sizeReal
    private int sizeReal;

    private final static int keyLength = Long.BYTES + Long.BYTES;
    private final static int valueLength = Integer.BYTES;

    public TimerMessageRecord() {
    }

    public TimerMessageRecord(long delayTime, long offsetPY, int sizeReal) {
        this.delayTime = delayTime;
        this.offsetPY = offsetPY;
        this.sizeReal = sizeReal;
    }

    @JSONField(serialize = false)
    public byte[] getKeyBytes() {
        byte[] keyBytes = new byte[keyLength];
        ByteBuffer buffer = ByteBuffer.wrap(keyBytes);
        buffer.putLong(this.getDelayTime()).putLong(this.getOffsetPY());
        return keyBytes;
    }

    @JSONField(serialize = false)
    public byte[] getValueBytes() {
        byte[] valueBytes = new byte[valueLength];
        ByteBuffer buffer = ByteBuffer.wrap(valueBytes);
        buffer.putInt(this.getSizeReal());
        return valueBytes;
    }

    public static TimerMessageRecord decode(byte[] body) {
        TimerMessageRecord timerMessageRecord = new TimerMessageRecord();
        ByteBuffer buffer = ByteBuffer.wrap(body);
        timerMessageRecord.setDelayTime(buffer.getLong());
        timerMessageRecord.setOffsetPY(buffer.getLong());
        timerMessageRecord.setSizeReal(buffer.getInt());
        return timerMessageRecord;
    }

    public void setDelayTime(long delayTime) {
        this.delayTime = delayTime;
    }

    public void setOffsetPY(long offsetPY) {
        this.offsetPY = offsetPY;
    }

    public void setSizeReal(int sizeReal) {
        this.sizeReal = sizeReal;
    }

    public int getSizeReal() {
        return sizeReal;
    }

    public long getDelayTime() {
        return delayTime;
    }

    public long getOffsetPY() {
        return offsetPY;
    }
}
