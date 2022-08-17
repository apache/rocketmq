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

import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.atomic.AtomicInteger;

public class NamedForkJoinWorkerThreadFactory implements ForkJoinPool.ForkJoinWorkerThreadFactory {
    private final String namePrefix;
    private final AtomicInteger counter = new AtomicInteger(0);

    NamedForkJoinWorkerThreadFactory(String namePrefix) {
        this.namePrefix = namePrefix;
    }

    public NamedForkJoinWorkerThreadFactory(String namePrefix, BrokerIdentity brokerIdentity) {
        if (brokerIdentity != null && brokerIdentity.isInBrokerContainer()) {
            this.namePrefix = brokerIdentity.getLoggerIdentifier() + namePrefix;
        } else {
            this.namePrefix = namePrefix;
        }
    }

    @Override
    public ForkJoinWorkerThread newThread(ForkJoinPool pool) {
        final ForkJoinWorkerThread worker = ForkJoinPool.defaultForkJoinWorkerThreadFactory.newThread(pool);
        worker.setName(namePrefix + counter.incrementAndGet());
        return worker;
    }
}
