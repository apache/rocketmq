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

package org.apache.rocketmq.proxy.processor.channel;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class RemoteChannelTest {

    @Test
    public void testEncodeAndDecode() {
        String remoteProxyIp = "11.193.0.1";
        String remoteAddress = "10.152.39.53:9768";
        String localAddress = "11.193.0.1:1210";
        ChannelProtocolType type = ChannelProtocolType.REMOTING;
        String extendAttribute = RandomStringUtils.randomAlphabetic(10);
        RemoteChannel remoteChannel = new RemoteChannel(remoteProxyIp, remoteAddress, localAddress, type, extendAttribute);

        String encodedData = remoteChannel.encode();
        assertNotNull(encodedData);

        RemoteChannel decodedChannel = RemoteChannel.decode(encodedData);
        assertEquals(remoteProxyIp, decodedChannel.remoteProxyIp);
        assertEquals(remoteAddress, decodedChannel.getRemoteAddress());
        assertEquals(localAddress, decodedChannel.getLocalAddress());
        assertEquals(type, decodedChannel.type);
        assertEquals(extendAttribute, decodedChannel.extendAttribute);

        assertNull(RemoteChannel.decode(""));
    }
}