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

package org.apache.rocketmq.grpc.common;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DelayPolicy {
    private List<Long> delayIntervalList;

    private DelayPolicy(List<Long> delayIntervalList) {
        this.delayIntervalList = delayIntervalList;
    }

    public long getDelayInterval(int index) {
        int size = delayIntervalList.size();
        if (index >= size) {
            throw new IllegalArgumentException("Out of index, size: " + size);
        }
        return delayIntervalList.get(index);
    }

    public void refresh(String messageDelayLevel) {
        delayIntervalList = buildList(messageDelayLevel);
    }

    public static DelayPolicy build(String messageDelayLevel) {
        return new DelayPolicy(buildList(messageDelayLevel));
    }

    private static List<Long> buildList(String messageDelayLevel) {
        List<String> delayLevelList = Lists.newArrayList(Splitter.on(" ").split(messageDelayLevel));
        List<Long> delayIntervalList = new ArrayList<>();
        for (String delayLevel : delayLevelList) {
            final Pattern p = Pattern.compile("(\\d+)([smhd])");
            final Matcher m = p.matcher(delayLevel);
            while (m.find())
            {
                final int duration = Integer.parseInt(m.group(1));
                final String timeUnitString = m.group(2);
                final long interval = toInterval(duration, timeUnitString);
                delayIntervalList.add(interval);
            }
        }
        return delayIntervalList;
    }

    private static long toInterval(int duration, final String timeUnitString) {
        switch (timeUnitString) {
            case "s":
                return TimeUnit.SECONDS.toMillis(duration);
            case "m":
                return TimeUnit.MINUTES.toMillis(duration);
            case "h":
                return TimeUnit.HOURS.toMillis(duration);
            case "d":
                return TimeUnit.DAYS.toMillis(duration);
            default:
                throw new IllegalArgumentException(String.format("%s is not a valid code [smhd]", timeUnitString));
        }
    }
}
