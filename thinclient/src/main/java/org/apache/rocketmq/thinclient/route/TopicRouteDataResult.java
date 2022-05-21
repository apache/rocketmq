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

package org.apache.rocketmq.thinclient.route;

import apache.rocketmq.v2.Status;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

import javax.annotation.concurrent.Immutable;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Result topic route data fetched from remote.
 */
@Immutable
public class TopicRouteDataResult {
    private final TopicRouteData topicRouteData;
    private final Status status;

    public TopicRouteDataResult(TopicRouteData topicRouteData, Status status) {
        this.topicRouteData = checkNotNull(topicRouteData, "topicRouteData should not be null");
        this.status = checkNotNull(status, "status should not be null");
    }

    public TopicRouteData getTopicRouteData() {
        return topicRouteData;
    }

    public Status getStatus() {
        return status;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TopicRouteDataResult result = (TopicRouteDataResult) o;
        return Objects.equal(topicRouteData, result.topicRouteData) && Objects.equal(status, result.status);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(topicRouteData, status);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("topicRouteData", topicRouteData)
            .add("code", status.getCode().getNumber())
            .add("message", status.getMessage())
            .toString();
    }
}
