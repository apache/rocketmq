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

package org.apache.rocketmq.broker.filter;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.rocketmq.filter.expression.Expression;
import org.apache.rocketmq.filter.util.BloomFilterData;

import java.util.Collections;

/**
 * Filter data of consumer.
 */
public class ConsumerFilterData {

    private String consumerGroup;
    private String topic;
    private String expression;
    private String expressionType;
    private transient Expression compiledExpression;
    private long bornTime;
    private long deadTime = 0;
    private BloomFilterData bloomFilterData;
    private long clientVersion;

    public boolean isDead() {
        return this.deadTime >= this.bornTime;
    }

    public long howLongAfterDeath() {
        if (isDead()) {
            return System.currentTimeMillis() - getDeadTime();
        }
        return -1;
    }

    /**
     * Check this filter data has been used to calculate bit map when msg was stored in server.
     */
    public boolean isMsgInLive(long msgStoreTime) {
        return msgStoreTime > getBornTime();
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public void setConsumerGroup(final String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(final String topic) {
        this.topic = topic;
    }

    public String getExpression() {
        return expression;
    }

    public void setExpression(final String expression) {
        this.expression = expression;
    }

    public String getExpressionType() {
        return expressionType;
    }

    public void setExpressionType(final String expressionType) {
        this.expressionType = expressionType;
    }

    public Expression getCompiledExpression() {
        return compiledExpression;
    }

    public void setCompiledExpression(final Expression compiledExpression) {
        this.compiledExpression = compiledExpression;
    }

    public long getBornTime() {
        return bornTime;
    }

    public void setBornTime(final long bornTime) {
        this.bornTime = bornTime;
    }

    public long getDeadTime() {
        return deadTime;
    }

    public void setDeadTime(final long deadTime) {
        this.deadTime = deadTime;
    }

    public BloomFilterData getBloomFilterData() {
        return bloomFilterData;
    }

    public void setBloomFilterData(final BloomFilterData bloomFilterData) {
        this.bloomFilterData = bloomFilterData;
    }

    public long getClientVersion() {
        return clientVersion;
    }

    public void setClientVersion(long clientVersion) {
        this.clientVersion = clientVersion;
    }

    @Override
    public boolean equals(Object o) {
        return EqualsBuilder.reflectionEquals(this, o, Collections.<String>emptyList());
    }

    @Override
    public int hashCode() {
        return HashCodeBuilder.reflectionHashCode(this, Collections.<String>emptyList());
    }

    @Override
    public String toString() {
        return ReflectionToStringBuilder.toString(this, ToStringStyle.SHORT_PREFIX_STYLE);
    }
}
