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

package org.apache.rocketmq.remoting.protocol.body;

public class ConsumeQueueData {

    private long physicOffset;
    private int physicSize;
    private long tagsCode;
    private String extendDataJson;
    private String bitMap;
    private boolean eval;
    private String msg;

    public long getPhysicOffset() {
        return physicOffset;
    }

    public void setPhysicOffset(long physicOffset) {
        this.physicOffset = physicOffset;
    }

    public int getPhysicSize() {
        return physicSize;
    }

    public void setPhysicSize(int physicSize) {
        this.physicSize = physicSize;
    }

    public long getTagsCode() {
        return tagsCode;
    }

    public void setTagsCode(long tagsCode) {
        this.tagsCode = tagsCode;
    }

    public String getExtendDataJson() {
        return extendDataJson;
    }

    public void setExtendDataJson(String extendDataJson) {
        this.extendDataJson = extendDataJson;
    }

    public String getBitMap() {
        return bitMap;
    }

    public void setBitMap(String bitMap) {
        this.bitMap = bitMap;
    }

    public boolean isEval() {
        return eval;
    }

    public void setEval(boolean eval) {
        this.eval = eval;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    @Override
    public String toString() {
        return "ConsumeQueueData{" +
            "physicOffset=" + physicOffset +
            ", physicSize=" + physicSize +
            ", tagsCode=" + tagsCode +
            ", extendDataJson='" + extendDataJson + '\'' +
            ", bitMap='" + bitMap + '\'' +
            ", eval=" + eval +
            ", msg='" + msg + '\'' +
            '}';
    }
}
