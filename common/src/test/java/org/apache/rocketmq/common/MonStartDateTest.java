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

import org.junit.Assert;
import org.junit.Test;

import java.util.Calendar;

public class MonStartDateTest {

    @Test
    public void testMonStartTime() {

        //1488939776000 ==> 2017/3/8 10:22:56
        long AM = 1488939776000L;
        long monStartTime1 = getMonStartTime(AM);

        //1488958228000 ==> 2017/3/8 15:30:28
        long PM = 1488958228000L;
        long monStartTime2 = getMonStartTime(PM);

        Assert.assertEquals(1488297600000L, monStartTime1);
        Assert.assertEquals(1488297600000L, monStartTime2);
        Assert.assertEquals(monStartTime1, monStartTime2);
    }

    public static long getMonStartTime(long time) {

        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(time);
        cal.set(Calendar.DAY_OF_MONTH,1);
        cal.set(Calendar.HOUR_OF_DAY,0);
        cal.set(Calendar.MINUTE,0);
        cal.set(Calendar.SECOND,0);
        cal.set(Calendar.MILLISECOND,0);
        return cal.getTimeInMillis();
    }
}