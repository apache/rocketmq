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

import org.apache.rocketmq.logging.org.slf4j.Logger;

public class StatisticsItemPrinter {
    private Logger log;

    private StatisticsItemFormatter formatter;

    public StatisticsItemPrinter(StatisticsItemFormatter formatter, Logger log) {
        this.formatter = formatter;
        this.log = log;
    }

    public void log(Logger log) {
        this.log = log;
    }

    public void formatter(StatisticsItemFormatter formatter) {
        this.formatter = formatter;
    }

    public void print(String prefix, StatisticsItem statItem, String... suffixs) {
        StringBuilder suffix = new StringBuilder();
        for (String str : suffixs) {
            suffix.append(str);
        }

        if (log != null) {
            log.info("{}{}{}", prefix, formatter.format(statItem), suffix.toString());
        }
        // System.out.printf("%s %s%s%s\n", new Date().toString(), prefix, formatter.format(statItem), suffix.toString());
    }
}
