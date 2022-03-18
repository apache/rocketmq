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

package org.apache.rocketmq.apis.consumer;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

public class SubscriptionExpression {
    private final String topic;
    private final FilterExpression filterExpression;

    SubscriptionExpression(String topic, FilterExpression filterExpression) {
        this.topic = topic;
        this.filterExpression = filterExpression;
    }

    public static SubscriptionExpressionBuilder builder() {
        return new SubscriptionExpressionBuilder();
    }

    public String getTopic() {
        return topic;
    }

    public FilterExpression getFilterExpression() {
        return filterExpression;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SubscriptionExpression that = (SubscriptionExpression) o;
        return Objects.equal(topic, that.topic) && Objects.equal(filterExpression, that.filterExpression);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(topic, filterExpression);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                          .add("topic", topic)
                          .add("filterExpression", filterExpression)
                          .toString();
    }
}