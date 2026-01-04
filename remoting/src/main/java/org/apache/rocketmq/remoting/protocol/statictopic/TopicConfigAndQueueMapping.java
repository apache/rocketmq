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
package org.apache.rocketmq.remoting.protocol.statictopic;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.rocketmq.common.TopicConfig;

public class TopicConfigAndQueueMapping extends TopicConfig {
    private TopicQueueMappingDetail mappingDetail;

    public TopicConfigAndQueueMapping() {
    }

    public TopicConfigAndQueueMapping(TopicConfig topicConfig, TopicQueueMappingDetail mappingDetail) {
        super(topicConfig);
        this.mappingDetail = mappingDetail;
    }

    public TopicQueueMappingDetail getMappingDetail() {
        return mappingDetail;
    }

    public void setMappingDetail(TopicQueueMappingDetail mappingDetail) {
        this.mappingDetail = mappingDetail;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;

        if (!(o instanceof TopicConfigAndQueueMapping)) return false;

        TopicConfigAndQueueMapping that = (TopicConfigAndQueueMapping) o;

        return new EqualsBuilder()
                .appendSuper(super.equals(o))
                .append(mappingDetail, that.mappingDetail)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .appendSuper(super.hashCode())
                .append(mappingDetail)
                .toHashCode();
    }

    @Override
    public String toString() {
        String string = super.toString();
        if (StringUtils.isNotBlank(string)) {
            string = string.substring(0, string.length() - 1) + ", mappingDetail=" + mappingDetail + "]";
        }
        return string;
    }
}
