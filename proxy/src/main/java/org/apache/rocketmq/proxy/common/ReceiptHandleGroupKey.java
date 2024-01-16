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

package org.apache.rocketmq.proxy.common;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import io.netty.channel.Channel;

public class ReceiptHandleGroupKey {
    protected final Channel channel;
    protected final String group;

    public ReceiptHandleGroupKey(Channel channel, String group) {
        this.channel = channel;
        this.group = group;
    }

    protected String getChannelId() {
        return channel.id().asLongText();
    }

    public String getGroup() {
        return group;
    }

    public Channel getChannel() {
        return channel;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ReceiptHandleGroupKey key = (ReceiptHandleGroupKey) o;
        return Objects.equal(getChannelId(), key.getChannelId()) && Objects.equal(group, key.group);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(getChannelId(), group);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("channelId", getChannelId())
            .add("group", group)
            .toString();
    }
}
