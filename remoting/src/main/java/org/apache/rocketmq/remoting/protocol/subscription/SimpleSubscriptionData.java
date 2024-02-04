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

package org.apache.rocketmq.remoting.protocol.subscription;

import com.google.common.base.MoreObjects;
import java.util.Objects;

public class SimpleSubscriptionData {
    private String topic;
    private String expressionType;
    private String expression;
    private long version;

    public SimpleSubscriptionData(String topic, String expressionType, String expression, long version) {
        this.topic = topic;
        this.expressionType = expressionType;
        this.expression = expression;
        this.version = version;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getExpressionType() {
        return expressionType;
    }

    public void setExpressionType(String expressionType) {
        this.expressionType = expressionType;
    }

    public String getExpression() {
        return expression;
    }

    public void setExpression(String expression) {
        this.expression = expression;
    }

    public long getVersion() {
        return version;
    }

    public void setVersion(long version) {
        this.version = version;
    }

    @Override public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SimpleSubscriptionData that = (SimpleSubscriptionData) o;
        return version == that.version && Objects.equals(topic, that.topic);
    }

    @Override public int hashCode() {
        return Objects.hash(topic, version);
    }

    @Override public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("topic", topic)
            .add("expressionType", expressionType)
            .add("expression", expression)
            .add("version", version)
            .toString();
    }
}
