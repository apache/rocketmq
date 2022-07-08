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
package org.apache.rocketmq.client.consumer;

import java.util.List;
import org.apache.rocketmq.common.message.MessageExt;

public class PopResult {
    private List<MessageExt> msgFoundList;
    private PopStatus popStatus;
    private long popTime;
    private long invisibleTime;
    private long restNum;

    public PopResult(PopStatus popStatus, List<MessageExt> msgFoundList) {
        this.popStatus = popStatus;
        this.msgFoundList = msgFoundList;
    }

    public long getPopTime() {
        return popTime;
    }


    public void setPopTime(long popTime) {
        this.popTime = popTime;
    }

    public long getRestNum() {
        return restNum;
    }

    public void setRestNum(long restNum) {
        this.restNum = restNum;
    }

    public long getInvisibleTime() {
        return invisibleTime;
    }


    public void setInvisibleTime(long invisibleTime) {
        this.invisibleTime = invisibleTime;
    }


    public void setPopStatus(PopStatus popStatus) {
        this.popStatus = popStatus;
    }

    public PopStatus getPopStatus() {
        return popStatus;
    }

    public List<MessageExt> getMsgFoundList() {
        return msgFoundList;
    }

    public void setMsgFoundList(List<MessageExt> msgFoundList) {
        this.msgFoundList = msgFoundList;
    }

    @Override
    public String toString() {
        return "PopResult [popStatus=" + popStatus + ",msgFoundList="
            + (msgFoundList == null ? 0 : msgFoundList.size()) + ",restNum=" + restNum + "]";
    }
}
