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
package org.apache.rocketmq.broker;

import com.alibaba.fastjson.JSON;
import com.github.benmanes.caffeine.cache.RemovalCause;
import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.common.AsyncTask;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.TaskStatus;

public class AdminAsyncTaskManager {

    // taskId -> AsyncTask
    private final Cache<String, AsyncTask> asyncTaskCache;

    // taskName -> taskId
    private final ConcurrentHashMap<String, List<String>> taskNameToIdsMap;

    private final BrokerConfig brokerConfig;

    private final int taskCacheExpireTimeMinutes;

    private final int maxTaskCacheSize;

    public AdminAsyncTaskManager(BrokerConfig brokerConfig) {
        this.brokerConfig = brokerConfig;
        this.taskCacheExpireTimeMinutes = brokerConfig.getTaskCacheExpireTimeMinutes();
        this.maxTaskCacheSize = brokerConfig.getMaxTaskCacheSize();
        this.taskNameToIdsMap = new ConcurrentHashMap<>();
        this.asyncTaskCache = Caffeine.newBuilder()
            .expireAfterWrite(taskCacheExpireTimeMinutes, TimeUnit.MINUTES)
            .maximumSize(maxTaskCacheSize)
            .removalListener((String taskId, AsyncTask task, RemovalCause cause) -> {
                if (task != null) {
                    taskNameToIdsMap.computeIfPresent(task.getTaskName(), (k, list) -> {
                        list.remove(taskId);
                        return list.isEmpty() ? null : list;
                    });
                }
            })
            .build();
    }

    /**
     * Creates a new asynchronous task with a unique taskId.
     *
     * @param taskName The name of the task.
     * @param future The CompletableFuture representing the asynchronous task.
     * @return The generated taskId.
     */
    public String createTask(String taskName, CompletableFuture<?> future) {
        String taskId = UUID.randomUUID().toString();
        AsyncTask task = new AsyncTask(taskName, taskId, future);

        asyncTaskCache.put(taskId, task);
        taskNameToIdsMap.computeIfAbsent(taskName, k -> Collections.synchronizedList(new ArrayList<>())).add(taskId);

        future.whenComplete((result, throwable) -> {
            if (throwable != null) {
                task.setStatus(TaskStatus.ERROR.getValue());
                task.setResult(throwable.getMessage());
            } else {
                task.setStatus(TaskStatus.SUCCESS.getValue());
                task.setResult(JSON.toJSONString(result));
            }
        });

        return taskId;
    }

    /**
     * Get all taskIds associated with a given task name.
     *
     * @param taskName The name of the task.
     * @return List of taskIds for the given task name.
     */
    public List<String> getTaskIdsByName(String taskName) {
        return taskNameToIdsMap.getOrDefault(taskName, Collections.emptyList());
    }

    /**
     * Get the status of a specific task.
     *
     * @param taskId The unique identifier of the task.
     * @return The AsyncTask object, or null if not found.
     */
    public AsyncTask getTaskStatus(String taskId) {
        return asyncTaskCache.getIfPresent(taskId);
    }

    /**
     * Update the status and result of a specific task.
     *
     * @param taskId The unique identifier of the task.
     * @param status The new status of the task.
     * @param result The result of the task.
     */
    public void updateTaskStatus(String taskId, int status, String result) {
        AsyncTask task = asyncTaskCache.getIfPresent(taskId);
        if (task != null) {
            task.setStatus(status);
            task.setResult(result);
            asyncTaskCache.put(taskId, task);
        }
    }

    /**
     * Remove a specific task from the cache and mappings.
     *
     * @param taskId The unique identifier of the task.
     */
    public void removeTask(String taskId) {
        AsyncTask task = asyncTaskCache.getIfPresent(taskId);
        if (task != null) {
            asyncTaskCache.invalidate(taskId);
            taskNameToIdsMap.computeIfPresent(task.getTaskName(), (k, v) -> {
                v.remove(taskId);
                return v.isEmpty() ? null : v;
            });
        }
    }
}