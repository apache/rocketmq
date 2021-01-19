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
package org.apache.rocketmq.store.pop;

import java.util.ArrayList;
import java.util.List;

public class PopCheckPoint {
    long startOffset;
    long popTime;
    long invisibleTime;
    int bitMap;
    byte num;
    byte queueId;
    String topic;
    String cid;
    long reviveOffset;
    List<Integer> queueOffsetDiff;

    public long getRo() {
        return reviveOffset;
    }

    public void setRo(long reviveOffset) {
        this.reviveOffset = reviveOffset;
    }

    public long getSo() {
        return startOffset;
    }

    public void setSo(long startOffset) {
        this.startOffset = startOffset;
    }

    public void setPt(long popTime) {
        this.popTime = popTime;
    }

    public void setIt(long invisibleTime) {
        this.invisibleTime = invisibleTime;
    }

    public long getPt() {
        return popTime;
    }

    public long getIt() {
        return invisibleTime;
    }

    public long getRt() {
        return popTime + invisibleTime;
    }

    public int getBm() {
        return bitMap;
    }

    public void setBm(int bitMap) {
        this.bitMap = bitMap;
    }

    public byte getN() {
        return num;
    }

    public void setN(byte num) {
        this.num = num;
    }

    public byte getQ() {
        return queueId;
    }

    public void setQ(byte queueId) {
        this.queueId = queueId;
    }

    public String getT() {
        return topic;
    }

    public void setT(String topic) {
        this.topic = topic;
    }

    public String getC() {
        return cid;
    }

    public void setC(String cid) {
        this.cid = cid;
    }

    public List<Integer> getD() {
        return queueOffsetDiff;
    }

    public void setD(List<Integer> queueOffsetDiff) {
        this.queueOffsetDiff = queueOffsetDiff;
    }

    public void addDiff(int diff) {
        if (this.queueOffsetDiff == null) {
            this.queueOffsetDiff = new ArrayList<>(8);
        }
        this.queueOffsetDiff.add(diff);
    }

    public int indexOfAck(long ackOffset) {
        if (ackOffset < startOffset) {
            return -1;
        }

        // old version of checkpoint
        if (queueOffsetDiff == null || queueOffsetDiff.isEmpty()) {

            if (ackOffset - startOffset < num) {
                return (int) (ackOffset - startOffset);
            }

            return -1;
        }

        // new version of checkpoint
        return queueOffsetDiff.indexOf((int) (ackOffset - startOffset));
    }

    public long ackOffsetByIndex(byte index) {
        // old version of checkpoint
        if (queueOffsetDiff == null || queueOffsetDiff.isEmpty()) {
            return startOffset + index;
        }

        return startOffset + queueOffsetDiff.get(index);
    }

    @Override
    public String toString() {
        return "PopCheckPoint [topic=" + topic + ", cid=" + cid + ", queueId=" + queueId + ", startOffset=" + startOffset + ", bitMap=" + bitMap + ", num=" + num + ", reviveTime=" + getRt()
            + ", reviveOffset=" + reviveOffset + ", diff=" + queueOffsetDiff + "]";
    }

}
