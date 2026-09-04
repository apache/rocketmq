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
package org.apache.rocketmq.remoting.protocol.body;

import java.util.List;
import org.apache.rocketmq.remoting.annotation.CFNotNull;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

public class DeleteSubscriptionGroupListRequestBody extends RemotingSerializable {
    @CFNotNull
    private List<String> groupNameList;

    private boolean cleanOffset = false;

    public DeleteSubscriptionGroupListRequestBody() {
    }

    public DeleteSubscriptionGroupListRequestBody(List<String> groupNameList) {
        this.groupNameList = groupNameList;
    }

    public DeleteSubscriptionGroupListRequestBody(List<String> groupNameList, boolean cleanOffset) {
        this.groupNameList = groupNameList;
        this.cleanOffset = cleanOffset;
    }

    public List<String> getGroupNameList() {
        return groupNameList;
    }

    public void setGroupNameList(List<String> groupNameList) {
        this.groupNameList = groupNameList;
    }

    public boolean isCleanOffset() {
        return cleanOffset;
    }

    public void setCleanOffset(boolean cleanOffset) {
        this.cleanOffset = cleanOffset;
    }
}
