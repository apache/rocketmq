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
package org.apache.rocketmq.store.stats;

import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BrokerStats {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);

    private final DefaultMessageStore defaultMessageStore;

    private volatile long msgPutTotalYesterdayMorning;

    private volatile long msgPutTotalTodayMorning;

    private volatile long msgGetTotalYesterdayMorning;

    private volatile long msgGetTotalTodayMorning;

    public BrokerStats(DefaultMessageStore defaultMessageStore) {
        this.defaultMessageStore = defaultMessageStore;
    }

    public void record() {
        this.msgPutTotalYesterdayMorning = this.msgPutTotalTodayMorning;
        this.msgGetTotalYesterdayMorning = this.msgGetTotalTodayMorning;

        this.msgPutTotalTodayMorning =
            this.defaultMessageStore.getStoreStatsService().getPutMessageTimesTotal();
        this.msgGetTotalTodayMorning =
            this.defaultMessageStore.getStoreStatsService().getGetMessageTransferedMsgCount().get();

        log.info("yesterday put message total: {}", msgPutTotalTodayMorning - msgPutTotalYesterdayMorning);
        log.info("yesterday get message total: {}", msgGetTotalTodayMorning - msgGetTotalYesterdayMorning);
    }

    public long getMsgPutTotalYesterdayMorning() {
        return msgPutTotalYesterdayMorning;
    }

    public void setMsgPutTotalYesterdayMorning(long msgPutTotalYesterdayMorning) {
        this.msgPutTotalYesterdayMorning = msgPutTotalYesterdayMorning;
    }

    public long getMsgPutTotalTodayMorning() {
        return msgPutTotalTodayMorning;
    }

    public void setMsgPutTotalTodayMorning(long msgPutTotalTodayMorning) {
        this.msgPutTotalTodayMorning = msgPutTotalTodayMorning;
    }

    public long getMsgGetTotalYesterdayMorning() {
        return msgGetTotalYesterdayMorning;
    }

    public void setMsgGetTotalYesterdayMorning(long msgGetTotalYesterdayMorning) {
        this.msgGetTotalYesterdayMorning = msgGetTotalYesterdayMorning;
    }

    public long getMsgGetTotalTodayMorning() {
        return msgGetTotalTodayMorning;
    }

    public void setMsgGetTotalTodayMorning(long msgGetTotalTodayMorning) {
        this.msgGetTotalTodayMorning = msgGetTotalTodayMorning;
    }

    public long getMsgPutTotalTodayNow() {
        return this.defaultMessageStore.getStoreStatsService().getPutMessageTimesTotal();
    }

    public long getMsgGetTotalTodayNow() {
        return this.defaultMessageStore.getStoreStatsService().getGetMessageTransferedMsgCount().get();
    }
}
