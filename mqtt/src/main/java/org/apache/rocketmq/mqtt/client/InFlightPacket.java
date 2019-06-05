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

package org.apache.rocketmq.mqtt.client;

import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

public class InFlightPacket implements Delayed {

    private final MQTTSession client;
    private final int packetId;
    private long startTime;
    private int resendTime = 0;

    InFlightPacket(MQTTSession client, int packetId, long delayInMilliseconds) {
        this.client = client;
        this.packetId = packetId;
        this.startTime = System.currentTimeMillis() + delayInMilliseconds;
    }

    @Override
    public long getDelay(TimeUnit unit) {
        long diff = startTime - System.currentTimeMillis();
        return unit.convert(diff, TimeUnit.MILLISECONDS);
    }

    @Override
    public int compareTo(Delayed o) {
        if ((this.startTime - ((InFlightPacket) o).startTime) == 0) {
            return 0;
        }
        if ((this.startTime - ((InFlightPacket) o).startTime) > 0) {
            return 1;
        } else {
            return -1;
        }
    }

    public MQTTSession getClient() {
        return client;
    }

    public int getPacketId() {
        return packetId;
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public int getResendTime() {
        return resendTime;
    }

    public void setResendTime(int resendTime) {
        this.resendTime = resendTime;
    }

    @Override public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof InFlightPacket)) {
            return false;
        }
        InFlightPacket packet = (InFlightPacket) obj;
        return packet.getClient().equals(this.getClient()) &&
            packet.getPacketId() == this.getPacketId();
    }
}