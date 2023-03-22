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

package org.apache.rocketmq.store.ha.io;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

public class HAWriter {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    protected final List<HAWriteHook> writeHookList = new ArrayList<>();

    public boolean write(SocketChannel socketChannel, ByteBuffer byteBufferWrite) throws IOException {
        int writeSizeZeroTimes = 0;
        while (byteBufferWrite.hasRemaining()) {
            int writeSize = socketChannel.write(byteBufferWrite);
            for (HAWriteHook writeHook : writeHookList) {
                writeHook.afterWrite(writeSize);
            }
            if (writeSize > 0) {
                writeSizeZeroTimes = 0;
            } else if (writeSize == 0) {
                if (++writeSizeZeroTimes >= 3) {
                    break;
                }
            } else {
                LOGGER.info("Write socket < 0");
            }
        }

        return !byteBufferWrite.hasRemaining();
    }

    public void registerHook(HAWriteHook writeHook) {
        writeHookList.add(writeHook);
    }

    public void clearHook() {
        writeHookList.clear();
    }
}
