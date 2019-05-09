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
package org.apache.rocketmq.client.impl.consumer;

import java.util.List;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.PullStatus;
import org.apache.rocketmq.common.message.MessageExt;

public class PullResultExt extends PullResult {
    private final long suggestWhichBrokerId;
    private byte[] messageBinary;

    /**
     *
     * @param pullStatus  拉取状态
     * @param nextBeginOffset 下次拉取的start offset
     * @param minOffset       拉取的最小偏移量
     * @param maxOffset       路拉取的最大偏移量
     * @param msgFoundList     null
     * @param suggestWhichBrokerId  从哪个broker 拉取的
     * @param messageBinary         拉取消息的二进制
     */
    public PullResultExt(PullStatus pullStatus, long nextBeginOffset, long minOffset, long maxOffset,
        List<MessageExt> msgFoundList, final long suggestWhichBrokerId, final byte[] messageBinary) {
        super(pullStatus, nextBeginOffset, minOffset, maxOffset, msgFoundList);
        this.suggestWhichBrokerId = suggestWhichBrokerId;
        this.messageBinary = messageBinary;
    }

    public byte[] getMessageBinary() {
        return messageBinary;
    }

    public void setMessageBinary(byte[] messageBinary) {
        this.messageBinary = messageBinary;
    }

    public long getSuggestWhichBrokerId() {
        return suggestWhichBrokerId;
    }
}
