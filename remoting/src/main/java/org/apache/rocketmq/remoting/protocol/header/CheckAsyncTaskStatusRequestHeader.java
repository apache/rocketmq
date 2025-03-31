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

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.action.Action;
import org.apache.rocketmq.common.action.RocketMQAction;
import org.apache.rocketmq.common.resource.ResourceType;
import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.protocol.RequestCode;

@RocketMQAction(value = RequestCode.CHECK_ASYNC_TASK_STATUS, resource = ResourceType.CLUSTER, action = Action.GET)
public class CheckAsyncTaskStatusRequestHeader implements CommandCustomHeader {

    private String taskName;

    private String taskId;

    private int maxLimit;  // Optional parameter for filtering return tasks nums.

    private Integer taskStatus;  // Optional parameter for filtering tasks with specific statuses

    @Override
    public void checkFields() throws RemotingCommandException {
        if (StringUtils.isBlank(taskName) && StringUtils.isBlank(taskId)) {
            throw new RemotingCommandException("taskName and taskId cannot be empty at the same time");
        }
        if (maxLimit < 0) {
            throw new RemotingCommandException("maxLimit cannot be less than 0.");
        }
        if (taskStatus != null && (taskStatus < 0 || taskStatus > 3)) {
            throw new RemotingCommandException("taskStatus must be between 0 and 3");
        }
    }

    public String getTaskName() {
        return taskName;
    }

    public void setTaskName(String taskName) {
        this.taskName = taskName;
    }

    public int getMaxLimit() {
        return maxLimit;
    }

    public void setMaxLimit(int maxLimit) {
        this.maxLimit = maxLimit;
    }

    public Integer getTaskStatus() {
        return taskStatus;
    }

    public void setTaskStatus(Integer taskStatus) {
        this.taskStatus = taskStatus;
    }

    public String getTaskId() {
        return taskId;
    }

    public void setTaskId(String taskId) {
        this.taskId = taskId;
    }
}
