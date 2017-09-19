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

package org.apache.rocketmq.remoting.api.protocol;

import org.apache.rocketmq.remoting.api.channel.ChannelHandlerContextWrapper;

public interface Protocol {
    /**
     * Minimum Viable Protocol
     */
    String MVP = "mvp";
    String HTTP2 = "http2";
    String WEBSOCKET = "websocket";

    byte MVP_MAGIC = 0x14;
    byte WEBSOCKET_MAGIC = 0x15;
    byte HTTP_2_MAGIC = 0x16;

    String name();

    byte type();

    void assembleHandler(ChannelHandlerContextWrapper ctx);
}
