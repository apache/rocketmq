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

import org.apache.rocketmq.common.action.Action;
import org.apache.rocketmq.common.action.RocketMQAction;
import org.apache.rocketmq.common.resource.ResourceType;
import org.apache.rocketmq.common.resource.RocketMQResource;
import org.apache.rocketmq.remoting.annotation.CFNotNull;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.rpc.TopicQueueRequestHeader;

@RocketMQAction(value = RequestCode.ACK_MESSAGE, action = Action.SUB)
public class AckMessageRequestHeader extends TopicQueueRequestHeader {
    @CFNotNull
    @RocketMQResource(ResourceType.GROUP)
    private String consumerGroup;
    @CFNotNull
    @RocketMQResource(ResourceType.TOPIC)
    private String topic;
    @CFNotNull
    private Integer queueId;
    @CFNotNull
    private String extraInfo;

    @CFNotNull
    private Long offset;

    private String liteTopic;

    @Override
    public void checkFields() throws RemotingCommandException {
    }

    public void setOffset(Long offset) {
        this.offset = offset;
    }

    public Long getOffset() {
        return offset;
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public void setExtraInfo(String extraInfo) {
        this.extraInfo = extraInfo;
    }

    public String getExtraInfo() {
        return extraInfo;
    }

    public void setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public Integer getQueueId() {
        return queueId;
    }

    public void setQueueId(Integer queueId) {
        this.queueId = queueId;
    }

    public String getLiteTopic() {
        return liteTopic;
    }

    public void setLiteTopic(String liteTopic) {
        this.liteTopic = liteTopic;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(352);
        sb.append("AckMessageRequestHeader{");
        boolean first = true;
        if (consumerGroup != null) {
            if (!first) {
                sb.append(", ");
            }
            sb.append("consumerGroup=").append(consumerGroup);
            first = false;
        }
        if (topic != null) {
            if (!first) {
                sb.append(", ");
            }
            sb.append("topic=").append(topic);
            first = false;
        }
        if (queueId != null) {
            if (!first) {
                sb.append(", ");
            }
            sb.append("queueId=").append(queueId);
            first = false;
        }
        if (extraInfo != null) {
            if (!first) {
                sb.append(", ");
            }
            sb.append("extraInfo=").append(extraInfo);
            first = false;
        }
        if (offset != null) {
            if (!first) {
                sb.append(", ");
            }
            sb.append("offset=").append(offset);
            first = false;
        }
        if (liteTopic != null) {
            if (!first) {
                sb.append(", ");
            }
            sb.append("liteTopic=").append(liteTopic);
            first = false;
        }
        sb.append('}');
        return sb.toString();
    }
}
