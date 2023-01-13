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

package org.apache.rocketmq.common.stats;

import java.util.concurrent.ScheduledExecutorService;
import org.apache.rocketmq.logging.org.slf4j.Logger;

/**
 * A StatItem for response time, the only difference between from StatsItem is it has a different log output.
 */
public class RTStatsItem extends StatsItem {

    public RTStatsItem(String statsName, String statsKey, ScheduledExecutorService scheduledExecutorService,
        Logger logger) {
        super(statsName, statsKey, scheduledExecutorService, logger);
    }

    /**
     *   For Response Time stat Item, the print detail should be a little different, TPS and SUM makes no sense.
     *   And we give a name "AVGRT" rather than AVGPT for value getAvgpt()
      */
    @Override
    protected String statPrintDetail(StatsSnapshot ss) {
        return String.format("TIMES: %d AVGRT: %.2f", ss.getTimes(), ss.getAvgpt());
    }
}
