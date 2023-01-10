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
 * $Id: DeleteTopicRequestHeader.java 1835 2013-05-16 02:00:50Z vintagewang@apache.org $
 */
package org.apache.rocketmq.remoting.protocol.header;

import com.google.common.base.MoreObjects;
import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.annotation.CFNotNull;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;

public class CloneGroupOffsetRequestHeader implements CommandCustomHeader {
    @CFNotNull
    private String srcGroup;
    @CFNotNull
    private String destGroup;
    private String topic;
    private boolean offline;

    @Override
    public void checkFields() throws RemotingCommandException {
    }

    public String getDestGroup() {
        return destGroup;
    }

    public void setDestGroup(String destGroup) {
        this.destGroup = destGroup;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getSrcGroup() {

        return srcGroup;
    }

    public void setSrcGroup(String srcGroup) {
        this.srcGroup = srcGroup;
    }

    public boolean isOffline() {
        return offline;
    }

    public void setOffline(boolean offline) {
        this.offline = offline;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("srcGroup", srcGroup)
            .add("destGroup", destGroup)
            .add("topic", topic)
            .add("offline", offline)
            .toString();
    }
}
