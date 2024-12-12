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

package org.apache.rocketmq.common.utils;

import io.netty.channel.Channel;
import java.net.InetAddress;
import java.net.InetSocketAddress;

public class ChannelUtil {
    private ChannelUtil() {
        // Prevent class from being instantiated from outside
    }

    @SuppressWarnings("unused")
    public static String getRemoteIp(Channel channel) {
        InetSocketAddress inetSocketAddress = (InetSocketAddress) channel.remoteAddress();
        if (inetSocketAddress == null) {
            return "";
        }
        final InetAddress inetAddr = inetSocketAddress.getAddress();
        return inetAddr != null ? inetAddr.getHostAddress() : inetSocketAddress.getHostName();
    }

}
