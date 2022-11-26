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
package org.apache.rocketmq.common.sysflag;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.rocketmq.common.message.MessageExt;
import org.junit.Test;

public class MessageSysFlagTest {

    @Test
    public void testGetBornHostLength() {
        MessageExt messageExt = new MessageExt();

        // IPv4 host = ip(4 byte) + port(4 byte)
        int bornHostLength = MessageSysFlag.getBornHostLength(messageExt.getSysFlag());
        assertThat(bornHostLength).isEqualTo(8);

        int bornHostIpLength = MessageSysFlag.getBornHostIpLength(messageExt.getSysFlag());
        assertThat(bornHostIpLength).isEqualTo(4);

        // IPv6 host = ip(16 byte) + port(4 byte)
        messageExt.setBornHostV6Flag();
        bornHostLength = MessageSysFlag.getBornHostLength(messageExt.getSysFlag());
        assertThat(bornHostLength).isEqualTo(20);

        bornHostIpLength = MessageSysFlag.getBornHostIpLength(messageExt.getSysFlag());
        assertThat(bornHostIpLength).isEqualTo(16);
    }

    @Test
    public void testGetStoreHostLength() {
        MessageExt messageExt = new MessageExt();

        // IPv4 host = ip(4 byte) + port(4 byte)
        int storeHostLength = MessageSysFlag.getStoreHostLength(messageExt.getSysFlag());
        assertThat(storeHostLength).isEqualTo(8);

        int storeHostIpLength = MessageSysFlag.getStoreHostIpLength(messageExt.getSysFlag());
        assertThat(storeHostIpLength).isEqualTo(4);

        // IPv6 host = ip(16 byte) + port(4 byte)
        messageExt.setStoreHostAddressV6Flag();
        storeHostLength = MessageSysFlag.getStoreHostLength(messageExt.getSysFlag());
        assertThat(storeHostLength).isEqualTo(20);

        storeHostIpLength = MessageSysFlag.getStoreHostIpLength(messageExt.getSysFlag());
        assertThat(storeHostIpLength).isEqualTo(16);
    }
}