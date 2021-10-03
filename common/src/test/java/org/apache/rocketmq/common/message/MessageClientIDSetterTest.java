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

package org.apache.rocketmq.common.message;

import org.apache.rocketmq.common.UtilAll;
import org.junit.Test;

import java.util.TimeZone;

import static org.assertj.core.api.Assertions.assertThat;

public class MessageClientIDSetterTest {

    @Test
    public void testGetIPStrFromID() {
        byte[] ip = UtilAll.getIP();
        String ipStr = (4 == ip.length) ? UtilAll.ipToIPv4Str(ip) : UtilAll.ipToIPv6Str(ip);

        String uniqID = MessageClientIDSetter.createUniqID();
        String ipStrFromID = MessageClientIDSetter.getIPStrFromID(uniqID);

        assertThat(ipStr).isEqualTo(ipStrFromID);
    }


    @Test
    public void testGetPidFromID() {
        // Temporary fix on MacOS
        short pid = (short) UtilAll.getPid();

        String uniqID = MessageClientIDSetter.createUniqID();
        short pidFromID = (short) MessageClientIDSetter.getPidFromID(uniqID);

        assertThat(pid).isEqualTo(pidFromID);
    }

    @Test
    public void testGetNearlyTimeFromID() {
        String id = MessageClientIDSetter.createUniqID();
        TimeZone.setDefault(TimeZone.getTimeZone("GMT+8"));
        long time1 = MessageClientIDSetter.getNearlyTimeFromID(id).getTime();
        TimeZone.setDefault(TimeZone.getTimeZone("GMT"));
        long time2 = MessageClientIDSetter.getNearlyTimeFromID(id).getTime();
        assertThat(time1).isEqualTo(time2);
        assertThat(Math.abs(time1 - System.currentTimeMillis())).isLessThan(1000);
    }
}
