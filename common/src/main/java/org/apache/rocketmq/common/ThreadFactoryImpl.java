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

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.utils.VirtualThreadFactorySupport;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

public class ThreadFactoryImpl implements ThreadFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.COMMON_LOGGER_NAME);

    private final AtomicLong threadIndex = new AtomicLong(0);
    private final String threadNamePrefix;
    private final boolean daemon;
    //Whether preferring virtual thread
    private final boolean preferVirtualThread;
    private final ThreadFactory defaultFactory;

    public ThreadFactoryImpl(final String threadNamePrefix) {
        this(threadNamePrefix, false);
    }

    public ThreadFactoryImpl(final String threadNamePrefix, final boolean daemon) {
        this(threadNamePrefix, daemon, null);
    }

    public ThreadFactoryImpl(final String threadNamePrefix, final BrokerIdentity brokerIdentity) {
        this(threadNamePrefix, false, brokerIdentity);
    }

    public ThreadFactoryImpl(final String threadNamePrefix,
                             final boolean daemon,
                             final BrokerIdentity brokerIdentity) {
        this(threadNamePrefix, daemon, brokerIdentity, false);
    }

    public ThreadFactoryImpl(final String threadNamePrefix,
                             final boolean daemon,
                             final BrokerIdentity brokerIdentity,
                             final boolean preferVirtualThread) {
        this.daemon = daemon;
        this.preferVirtualThread = preferVirtualThread;
        if (brokerIdentity != null && brokerIdentity.isInBrokerContainer()) {
            this.threadNamePrefix = brokerIdentity.getIdentifier() + threadNamePrefix;
        } else {
            this.threadNamePrefix = threadNamePrefix;
        }
        if (this.preferVirtualThread) {
            final ThreadFactory virtualThreadFactory = VirtualThreadFactorySupport.create(this.threadNamePrefix);
            this.defaultFactory = virtualThreadFactory != null ? virtualThreadFactory : Executors.defaultThreadFactory();
        } else {
            this.defaultFactory = Executors.defaultThreadFactory();
        }
    }

    @Override
    public Thread newThread(final Runnable r) {
        final Thread thread = this.defaultFactory.newThread(r);
        if (!this.preferVirtualThread) {
            thread.setDaemon(daemon);
            final String threadName = this.threadNamePrefix + threadIndex.getAndIncrement();
            thread.setName(threadName);
        }

        // Log all uncaught exception
        thread.setUncaughtExceptionHandler((t, e) ->
            LOGGER.error("[BUG] Thread has an uncaught exception, threadId={}, threadName={}",
                t.getId(), t.getName(), e));

        return thread;
    }
}
