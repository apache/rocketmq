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
package org.apache.rocketmq.common.protocol.body;

import org.apache.rocketmq.common.statictopic.TopicQueueMappingDetail;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

public class TopicQueueMappingBody extends RemotingSerializable {

    private boolean force;
    private int prevGen;
    private TopicQueueMappingDetail mappingDetail;

    public int getPrevGen() {
        return prevGen;
    }

    public void setPrevGen(int prevGen) {
        this.prevGen = prevGen;
    }

    public TopicQueueMappingDetail getMappingDetail() {
        return mappingDetail;
    }

    public void setMappingDetail(TopicQueueMappingDetail mappingDetail) {
        this.mappingDetail = mappingDetail;
    }

    public boolean isForce() {
        return force;
    }

    public void setForce(boolean force) {
        this.force = force;
    }
}
