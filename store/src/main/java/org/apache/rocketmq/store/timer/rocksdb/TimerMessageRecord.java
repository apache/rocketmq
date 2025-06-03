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

import org.apache.rocketmq.common.message.MessageExt;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class TimerMessageRecord {
    enum Flag {
        DEFAULT,
        TRANSACTION,
        POP
    }

    // key: delayTime + uniqueKey
    private long delayTime;
    private String uniqueKey;
    private MessageExt messageExt;
    private boolean roll = false;
    // value: sizeReal + offsetPY
    private int sizeReal;
    private long offsetPY;
    private long readOffset;
    private final static int VALUE_LENGTH = Integer.BYTES + Long.BYTES;

    public TimerMessageRecord() {
    }

    public TimerMessageRecord(long delayTime, String uniqueKey, long offsetPY, int sizeReal) {
        this.delayTime = delayTime;
        this.uniqueKey = uniqueKey;
        this.offsetPY = offsetPY;
        this.sizeReal = sizeReal;
    }

    public byte[] getKeyBytes() {
        byte[] value = uniqueKey.getBytes(StandardCharsets.UTF_8);
        int keyLength = Long.BYTES + value.length;
        byte[] keyBytes = new byte[keyLength];
        ByteBuffer buffer = ByteBuffer.wrap(keyBytes);
        buffer.putLong(delayTime);
        buffer.put(value);
        return keyBytes;
    }

    public byte[] getValueBytes() {
        byte[] valueBytes = new byte[VALUE_LENGTH];
        ByteBuffer buffer = ByteBuffer.wrap(valueBytes);
        buffer.putInt(this.getSizeReal());
        buffer.putLong(this.getOffsetPY());
        return valueBytes;
    }

    public static TimerMessageRecord decode(byte[] body) {
        TimerMessageRecord timerMessageRecord = new TimerMessageRecord();
        ByteBuffer buffer = ByteBuffer.wrap(body);
        timerMessageRecord.setSizeReal(buffer.getInt());
        timerMessageRecord.setOffsetPY(buffer.getLong());
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

    public boolean isRoll() {
        return roll;
    }

    public void setRoll(boolean roll) {
        this.roll = roll;
    }

    public MessageExt getMessageExt() {
        return messageExt;
    }

    public void setMessageExt(MessageExt messageExt) {
        this.messageExt = messageExt;
    }

    public String getUniqueKey() {
        return uniqueKey;
    }

    public void setUniqueKey(String uniqueKey) {
        this.uniqueKey = uniqueKey;
    }

    public void setReadOffset(long readOffset) {
        this.readOffset = readOffset;
    }

    public long getReadOffset() {
        return readOffset;
    }

    @Override
    public String toString() {
        return "TimerMessageRecord{" +
            "delayTime=" + delayTime +
            ", offsetPY=" + offsetPY +
            ", sizeReal=" + sizeReal +
            ", messageExt=" + messageExt +
            ", roll=" + roll +
            ", uniqueKey='" + uniqueKey + '\'' +
            '}';
    }
}
