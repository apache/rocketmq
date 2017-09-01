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

package org.apache.rocketmq.common;

import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

public class TracerTime extends RemotingSerializable {

    private long messageCreateTime;
    private long messageSendTime;
    private long messageArriveBrokerTime;
    private long messageBeginSaveTime;
    private long messageSaveEndTime;
    private long brokerSendAckTime;
    private long receiveSendAckTime;

    public TracerTime() {
    }

    public long getMessageCreateTime() {
        return messageCreateTime;
    }

    public void setMessageCreateTime(long messageCreateTime) {
        this.messageCreateTime = messageCreateTime;
    }

    public long getMessageSendTime() {
        return messageSendTime;
    }

    public void setMessageSendTime(long messageSendTime) {
        this.messageSendTime = messageSendTime;
    }

    public long getMessageArriveBrokerTime() {
        return messageArriveBrokerTime;
    }

    public void setMessageArriveBrokerTime(long messageArriveBrokerTime) {
        this.messageArriveBrokerTime = messageArriveBrokerTime;
    }

    public long getMessageBeginSaveTime() {
        return messageBeginSaveTime;
    }

    public void setMessageBeginSaveTime(long messageBeginSaveTime) {
        this.messageBeginSaveTime = messageBeginSaveTime;
    }

    public long getMessageSaveEndTime() {
        return messageSaveEndTime;
    }

    public void setMessageSaveEndTime(long messageSaveEndTime) {
        this.messageSaveEndTime = messageSaveEndTime;
    }

    public long getBrokerSendAckTime() {
        return brokerSendAckTime;
    }

    public void setBrokerSendAckTime(long brokerSendAckTime) {
        this.brokerSendAckTime = brokerSendAckTime;
    }

    public long getReceiveSendAckTime() {
        return receiveSendAckTime;
    }

    public void setReceiveSendAckTime(long receiveSendAckTime) {
        this.receiveSendAckTime = receiveSendAckTime;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        TracerTime that = (TracerTime) o;

        if (messageCreateTime != that.messageCreateTime)
            return false;
        if (messageSendTime != that.messageSendTime)
            return false;
        if (messageArriveBrokerTime != that.messageArriveBrokerTime)
            return false;
        if (messageBeginSaveTime != that.messageBeginSaveTime)
            return false;
        if (messageSaveEndTime != that.messageSaveEndTime)
            return false;
        if (brokerSendAckTime != that.brokerSendAckTime)
            return false;
        return receiveSendAckTime == that.receiveSendAckTime;
    }

    @Override
    public int hashCode() {
        int result = (int) (messageCreateTime ^ (messageCreateTime >>> 32));
        result = 31 * result + (int) (messageSendTime ^ (messageSendTime >>> 32));
        result = 31 * result + (int) (messageArriveBrokerTime ^ (messageArriveBrokerTime >>> 32));
        result = 31 * result + (int) (messageBeginSaveTime ^ (messageBeginSaveTime >>> 32));
        result = 31 * result + (int) (messageSaveEndTime ^ (messageSaveEndTime >>> 32));
        result = 31 * result + (int) (brokerSendAckTime ^ (brokerSendAckTime >>> 32));
        result = 31 * result + (int) (receiveSendAckTime ^ (receiveSendAckTime >>> 32));
        return result;
    }
}
