/**
 * Copyright (C) 2010-2013 Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.rocketmq.broker.digestlog;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import com.alibaba.rocketmq.broker.BrokerController;


/**
 * 统计管理器
 * 
 * @author 菱叶<jin.qian@alipay.com>
 * @since 2013-7-18
 */
public class DigestLogManager {

    private final boolean startRealTimeStat = Boolean.valueOf(System
        .getProperty("meta.realtime.stat", "true"));
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1,
        new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, "DigestLogPrintSchedule");
            }
        });
    private final PutStatsMoniter putStatsMoniter;
    private final GetStatsMoniter getStatsMoniter;
    private final StoreStatsMoniter storeStatsMoniter;


    public DigestLogManager(BrokerController brokerController) {
        putStatsMoniter = new PutStatsMoniter(brokerController);
        getStatsMoniter = new GetStatsMoniter(brokerController);
        storeStatsMoniter = new StoreStatsMoniter(brokerController);
    }


    public void init() {

    }


    public void start() {
        if (startRealTimeStat) {
            scheduler.scheduleWithFixedDelay(new DigestPrintOut(), 10, 20, TimeUnit.SECONDS);
            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    scheduler.shutdown();
                }
            });
        }
    }


    public void dispose() {
        scheduler.shutdown();
    }

    class DigestPrintOut implements Runnable {
        @Override
        public void run() {
            putStatsMoniter.tolog();
            getStatsMoniter.tolog();
            storeStatsMoniter.tolog();
        }
    }


    public PutStatsMoniter getPutStatsMoniter() {
        return putStatsMoniter;
    }


    public StoreStatsMoniter getStoreStatsMoniter() {
        return storeStatsMoniter;
    }


    public GetStatsMoniter getGetStatsMoniter() {
        return getStatsMoniter;
    }

}
