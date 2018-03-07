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

package org.apache.rocketmq.broker;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.common.ClientTracerTimeUtil;
import org.apache.rocketmq.common.TracerTime;

public class ServerTracerTimeUtil {

    public static Cache<String, TracerTime> tracerTimeCache = CacheBuilder.newBuilder()
        .maximumSize(10000)
        .expireAfterWrite(15, TimeUnit.MINUTES)
        .build();

    public static boolean isEnableTracerTime() {
        return ClientTracerTimeUtil.isEnableTracerTime();
    }

    public static void addMessageCreateTime(String messageTracerTimeId, String messageCreateTime) {
        if (messageCreateTime == null || messageCreateTime.length() < 1) {
            return;
        }

        TracerTime tracerTime = tracerTimeCache.getIfPresent(messageTracerTimeId);
        if (tracerTime == null) {
            tracerTime = new TracerTime();
        }

        tracerTime.setMessageCreateTime(Long.valueOf(messageCreateTime));

        tracerTimeCache.put(messageTracerTimeId, tracerTime);
    }

    public static void addMessageSendTime(String messageTracerTimeId, String messageSendTime) {
        TracerTime tracerTime = tracerTimeCache.getIfPresent(messageTracerTimeId);
        if (tracerTime == null) {
            return;
        }
        if (messageSendTime == null || messageSendTime.length() < 1) {
            return;
        }
        tracerTime.setMessageSendTime(Long.valueOf(messageSendTime));
    }

    public static void addMessageArriveBrokerTime(String messageTracerTimeId, long messageArriveBrokerTime) {
        TracerTime tracerTime = tracerTimeCache.getIfPresent(messageTracerTimeId);
        if (tracerTime == null) {
            return;
        }

        tracerTime.setMessageArriveBrokerTime(messageArriveBrokerTime);
    }

    public static void addMessageBeginSaveTime(String messageTracerTimeId, long messageBeginSaveTime) {
        TracerTime tracerTime = tracerTimeCache.getIfPresent(messageTracerTimeId);
        if (tracerTime == null) {
            return;
        }

        tracerTime.setMessageBeginSaveTime(messageBeginSaveTime);
    }

    public static void addMessageSaveEndTime(String messageTracerTimeId, long messageSaveEndTime) {
        TracerTime tracerTime = tracerTimeCache.getIfPresent(messageTracerTimeId);
        if (tracerTime == null) {
            return;
        }

        tracerTime.setMessageSaveEndTime(messageSaveEndTime);
    }

    public static void addBrokerSendAckTime(String messageTracerTimeId, long brokerSendAckTime) {
        TracerTime tracerTime = tracerTimeCache.getIfPresent(messageTracerTimeId);
        if (tracerTime == null) {
            return;
        }
        tracerTime.setBrokerSendAckTime(brokerSendAckTime);
    }

    public static void addReceiveSendAckTime(String messageTracerTimeId, String receiveSendAckTime) {
        TracerTime tracerTime = tracerTimeCache.getIfPresent(messageTracerTimeId);
        if (tracerTime == null) {
            return;
        }
        if (receiveSendAckTime == null || receiveSendAckTime.length() < 1) {
            return;
        }
        tracerTime.setReceiveSendAckTime(Long.valueOf(receiveSendAckTime));
    }
}
