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
package org.apache.rocketmq.common.protocol.route;

import com.google.common.base.Objects;

public class TopicRouteDataNameSrv extends TopicRouteData {
    private LogicalQueuesInfoUnordered logicalQueuesInfoUnordered;

    public TopicRouteDataNameSrv() {
    }

    public LogicalQueuesInfoUnordered getLogicalQueuesInfoUnordered() {
        return logicalQueuesInfoUnordered;
    }

    public void setLogicalQueuesInfoUnordered(
        LogicalQueuesInfoUnordered logicalQueuesInfoUnordered) {
        this.logicalQueuesInfoUnordered = logicalQueuesInfoUnordered;
    }

    @Override public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        if (!super.equals(o))
            return false;
        TopicRouteDataNameSrv srv = (TopicRouteDataNameSrv) o;
        return Objects.equal(logicalQueuesInfoUnordered, srv.logicalQueuesInfoUnordered);
    }

    @Override public int hashCode() {
        return Objects.hashCode(super.hashCode(), logicalQueuesInfoUnordered);
    }

    @Override public String toString() {
        return "TopicRouteDataNameSrv{" +
            "logicalQueuesInfoUnordered=" + logicalQueuesInfoUnordered +
            "} " + super.toString();
    }

    public TopicRouteData toTopicRouteData() {
        TopicRouteData topicRouteData = new TopicRouteData(this);
        if (this.logicalQueuesInfoUnordered != null) {
            topicRouteData.setLogicalQueuesInfo(this.logicalQueuesInfoUnordered.toLogicalQueuesInfoOrdered());
        }
        return topicRouteData;
    }
}
