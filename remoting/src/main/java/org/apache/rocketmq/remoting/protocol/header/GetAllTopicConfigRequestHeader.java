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

package org.apache.rocketmq.remoting.protocol.header;

import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.annotation.CFNotNull;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;

/**
 * This request targets the complete topic configuration set and does not identify a single topic.
 *
 * <p>This header must not declare {@code @RocketMQAction}: it carries no resource fields, and the
 * typed {@code Topic:ANY + LIST} resource is constructed by {@code DefaultAuthorizationContextBuilder}.
 */
public class GetAllTopicConfigRequestHeader implements CommandCustomHeader {
    @Override
    public void checkFields() throws RemotingCommandException {
        // nothing
    }

    @CFNotNull
    private Integer topicSeq;

    private String dataVersion;

    private Integer maxTopicNum;

    public Integer getTopicSeq() {
        return topicSeq;
    }

    public void setTopicSeq(Integer topicSeq) {
        this.topicSeq = topicSeq;
    }

    public String getDataVersion() {
        return dataVersion;
    }

    public void setDataVersion(String dataVersion) {
        this.dataVersion = dataVersion;
    }

    public Integer getMaxTopicNum() {
        return maxTopicNum;
    }

    public void setMaxTopicNum(Integer maxTopicNum) {
        this.maxTopicNum = maxTopicNum;
    }
}
