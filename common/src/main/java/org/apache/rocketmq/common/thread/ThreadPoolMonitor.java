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

package org.apache.rocketmq.common.thread;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;

public class ThreadPoolMonitor {
    private static InternalLogger jstackLogger = InternalLoggerFactory.getLogger(ThreadPoolMonitor.class);
    private static InternalLogger waterMarkLogger = InternalLoggerFactory.getLogger(ThreadPoolMonitor.class);

    private static final List<ThreadPoolWrapper> MONITOR_EXECUTOR = new CopyOnWriteArrayList<>();
    private static final ScheduledExecutorService MONITOR_SCHEDULED = Executors.newSingleThreadScheduledExecutor(
        new ThreadFactoryBuilder().setNameFormat("ThreadPoolMonitor-%d").build()
    );

    private static volatile long threadPoolStatusPeriodTime = TimeUnit.SECONDS.toMillis(3);
    private static volatile boolean enablePrintJstack = true;
    private static volatile long jstackPeriodTime = 60000;
    private static volatile long jstackTime = System.currentTimeMillis();

    public static void config(InternalLogger jstackLoggerConfig, InternalLogger waterMarkLoggerConfig,
        boolean enablePrintJstack, long jstackPeriodTimeConfig, long threadPoolStatusPeriodTimeConfig) {
        jstackLogger = jstackLoggerConfig;
        waterMarkLogger = waterMarkLoggerConfig;
        threadPoolStatusPeriodTime = threadPoolStatusPeriodTimeConfig;
        ThreadPoolMonitor.enablePrintJstack = enablePrintJstack;
        jstackPeriodTime = jstackPeriodTimeConfig;
    }

    public static ThreadPoolExecutor createAndMonitor(int corePoolSize,
        int maximumPoolSize,
        long keepAliveTime,
        TimeUnit unit,
        String name,
        int queueCapacity) {
        return createAndMonitor(corePoolSize, maximumPoolSize, keepAliveTime, unit, name, queueCapacity, Collections.emptyList());
    }

    public static ThreadPoolExecutor createAndMonitor(int corePoolSize,
        int maximumPoolSize,
        long keepAliveTime,
        TimeUnit unit,
        String name,
        int queueCapacity,
        ThreadPoolStatusMonitor... threadPoolStatusMonitors) {
        return createAndMonitor(corePoolSize, maximumPoolSize, keepAliveTime, unit, name, queueCapacity,
            Lists.newArrayList(threadPoolStatusMonitors));
    }

    public static ThreadPoolExecutor createAndMonitor(int corePoolSize,
        int maximumPoolSize,
        long keepAliveTime,
        TimeUnit unit,
        String name,
        int queueCapacity,
        List<ThreadPoolStatusMonitor> threadPoolStatusMonitors) {
        ThreadPoolExecutor executor = new ThreadPoolExecutor(
            corePoolSize,
            maximumPoolSize,
            keepAliveTime,
            unit,
            new LinkedBlockingQueue<>(queueCapacity),
            new ThreadFactoryBuilder().setNameFormat(name + "-%d").build(),
            new ThreadPoolExecutor.DiscardOldestPolicy());
        List<ThreadPoolStatusMonitor> printers = Lists.newArrayList(new ThreadPoolQueueSizeMonitor(queueCapacity));
        printers.addAll(threadPoolStatusMonitors);

        MONITOR_EXECUTOR.add(ThreadPoolWrapper.builder()
            .name(name)
            .threadPoolExecutor(executor)
            .statusPrinters(printers)
            .build());
        return executor;
    }

    public static void logThreadPoolStatus() {
        for (ThreadPoolWrapper threadPoolWrapper : MONITOR_EXECUTOR) {
            List<ThreadPoolStatusMonitor> monitors = threadPoolWrapper.getStatusPrinters();
            for (ThreadPoolStatusMonitor monitor : monitors) {
                double value = monitor.value(threadPoolWrapper.getThreadPoolExecutor());
                waterMarkLogger.info("\t{}\t{}\t{}", threadPoolWrapper.getName(),
                    monitor.describe(),
                    value);

                if (enablePrintJstack) {
                    if (monitor.needPrintJstack(threadPoolWrapper.getThreadPoolExecutor(), value) &&
                        System.currentTimeMillis() - jstackTime > jstackPeriodTime) {
                        jstackTime = System.currentTimeMillis();
                        jstackLogger.warn("jstack start\n{}", UtilAll.jstack());
                    }
                }
            }
        }
    }

    public static void init() {
        MONITOR_SCHEDULED.scheduleAtFixedRate(ThreadPoolMonitor::logThreadPoolStatus, 20,
            threadPoolStatusPeriodTime, TimeUnit.MILLISECONDS);
    }

    public static void shutdown() {
        MONITOR_SCHEDULED.shutdown();
    }
}