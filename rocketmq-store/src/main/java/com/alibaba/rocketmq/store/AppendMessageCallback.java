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
package com.alibaba.rocketmq.store;

import java.nio.ByteBuffer;


/**
 * 写入消息的回调接口
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-7-21
 */
public interface AppendMessageCallback {

    /**
     * 序列化消息后，写入MapedByteBuffer
     * 
     * @param byteBuffer
     *            要写入的target
     * @param maxBlank
     *            要写入的target最大空白区
     * @param msg
     *            要写入的message
     * @return 写入多少字节
     */
    public AppendMessageResult doAppend(final long fileFromOffset, final ByteBuffer byteBuffer,
            final int maxBlank, final Object msg);
}
