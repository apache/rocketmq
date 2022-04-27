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

/**
 * $Id: PullMessageResponseHeader.java 1835 2013-05-16 02:00:50Z vintagewang@apache.org $
 */
package org.apache.rocketmq.common.protocol.header;

import java.util.HashMap;

import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.annotation.CFNotNull;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.protocol.FastCodesHeader;

import io.netty.buffer.ByteBuf;

public class PullMessageResponseHeader implements CommandCustomHeader, FastCodesHeader {
    @CFNotNull
    private Long suggestWhichBrokerId;
    @CFNotNull
    private Long nextBeginOffset;
    @CFNotNull
    private Long minOffset;
    @CFNotNull
    private Long maxOffset;

    @Override
    public void checkFields() throws RemotingCommandException {
    }

    @Override
    public void encode(ByteBuf out) {
        writeIfNotNull(out, "suggestWhichBrokerId", suggestWhichBrokerId);
        writeIfNotNull(out, "nextBeginOffset", nextBeginOffset);
        writeIfNotNull(out, "minOffset", minOffset);
        writeIfNotNull(out, "maxOffset", maxOffset);
    }

    @Override
    public void decode(HashMap<String, String> fields) throws RemotingCommandException {
        String str = getAndCheckNotNull(fields, "suggestWhichBrokerId");
        if (str != null) {
            this.suggestWhichBrokerId = Long.parseLong(str);
        }

        str = getAndCheckNotNull(fields, "nextBeginOffset");
        if (str != null) {
            this.nextBeginOffset = Long.parseLong(str);
        }

        str = getAndCheckNotNull(fields, "minOffset");
        if (str != null) {
            this.minOffset = Long.parseLong(str);
        }

        str = getAndCheckNotNull(fields, "maxOffset");
        if (str != null) {
            this.maxOffset = Long.parseLong(str);
        }
    }

    public Long getNextBeginOffset() {
        return nextBeginOffset;
    }

    public void setNextBeginOffset(Long nextBeginOffset) {
        this.nextBeginOffset = nextBeginOffset;
    }

    public Long getMinOffset() {
        return minOffset;
    }

    public void setMinOffset(Long minOffset) {
        this.minOffset = minOffset;
    }

    public Long getMaxOffset() {
        return maxOffset;
    }

    public void setMaxOffset(Long maxOffset) {
        this.maxOffset = maxOffset;
    }

    public Long getSuggestWhichBrokerId() {
        return suggestWhichBrokerId;
    }

    public void setSuggestWhichBrokerId(Long suggestWhichBrokerId) {
        this.suggestWhichBrokerId = suggestWhichBrokerId;
    }
}
