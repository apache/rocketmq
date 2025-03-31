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

package org.apache.rocketmq.common;

import java.util.Date;
import java.util.concurrent.CompletableFuture;

public class AsyncTask {

    private String taskName;

    private String taskId;

    private int status;

    private Date createTime;

    private String result;

    private final CompletableFuture<?> future;

    public AsyncTask(String taskName, String taskId, CompletableFuture<?> future) {
        this.taskName = taskName;
        this.taskId = taskId;
        this.status = TaskStatus.INIT.getValue();
        this.createTime = new Date();
        this.result = null;
        this.future = future;
    }

    public String getTaskName() {
        return taskName;
    }

    public void setTaskName(String taskName) {
        this.taskName = taskName;
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public String getResult() {
        return result;
    }

    public void setResult(String result) {
        this.result = result;
    }

    public Date getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Date createTime) {
        this.createTime = createTime;
    }

    public String getTaskId() {
        return taskId;
    }

    public void setTaskId(String taskId) {
        this.taskId = taskId;
    }

    public CompletableFuture<?> getFuture() {
        return future;
    }

    public static String getDescFromStatus(int status) {
        for (TaskStatus taskStatus : TaskStatus.values()) {
            if (taskStatus.getValue() == status) {
                return taskStatus.getDesc();
            }
        }
        return "unknown";
    }
}