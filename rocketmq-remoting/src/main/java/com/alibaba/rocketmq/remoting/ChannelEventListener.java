/**
 * Copyright (C) 2010-2013 Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.rocketmq.remoting;

import io.netty.channel.Channel;


/**
 * 监听Channel的事件，包括连接断开、连接建立、连接异常，传送这些事件到应用层
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-7-13
 */
public interface ChannelEventListener {
    public void onChannelConnect(final String remoteAddr, final Channel channel);


    public void onChannelClose(final String remoteAddr, final Channel channel);


    public void onChannelException(final String remoteAddr, final Channel channel);


    public void onChannelIdle(final String remoteAddr, final Channel channel);
}
