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
package org.apache.rocketmq.common.statistics;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class StatisticsItemScheduledPrinter extends FutureHolder {
    protected String name;

    protected StatisticsItemPrinter printer;
    protected ScheduledExecutorService executor;
    protected long interval;
    protected InitialDelay initialDelay;
    protected Valve valve;

    public StatisticsItemScheduledPrinter(String name, StatisticsItemPrinter printer,
                                          ScheduledExecutorService executor, InitialDelay initialDelay,
                                          long interval, Valve valve) {
        this.name = name;
        this.printer = printer;
        this.executor = executor;
        this.initialDelay = initialDelay;
        this.interval = interval;
        this.valve = valve;
    }

    /**
     * schedule a StatisticsItem to print all the values periodically
     */
    public void schedule(final StatisticsItem statisticsItem) {
        ScheduledFuture future = executor.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                if (enabled()) {
                    printer.print(name, statisticsItem);
                }
            }
        }, getInitialDelay(), interval, TimeUnit.MILLISECONDS);

        addFuture(statisticsItem, future);
    }

    public void remove(final StatisticsItem statisticsItem) {
        removeAllFuture(statisticsItem);
    }

    public interface InitialDelay {
        /**
         * Get initial delay value
         * @return
         */
        long get();
    }

    public interface Valve {
        /**
         * whether enabled
         * @return
         */
        boolean enabled();

        /**
         * whether print zero lines
         * @return
         */
        boolean printZeroLine();
    }

    protected long getInitialDelay() {
        return initialDelay != null ? initialDelay.get() : 0;
    }

    protected boolean enabled() {
        return valve != null ? valve.enabled() : false;
    }

    protected boolean printZeroLine() {
        return valve != null ? valve.printZeroLine() : false;
    }

}
