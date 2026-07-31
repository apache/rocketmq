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

package org.apache.rocketmq.proxy.grpc.admin.model;

/**
 * Route change event model.
 * Represents a single route change detected by the proxy.
 */
public class RouteChangeEvent {
    private RouteChangeEventType eventType;
    private long timestamp;
    private String topic;
    private String cluster;
    private String brokerName;
    private long brokerId;
    private String brokerAddress;
    private int previousReadQueueNums;
    private int currentReadQueueNums;
    private int previousWriteQueueNums;
    private int currentWriteQueueNums;
    private TopicRouteSnapshot routeSnapshot;

    public RouteChangeEvent() {
    }

    public RouteChangeEventType getEventType() {
        return eventType;
    }

    public void setEventType(RouteChangeEventType eventType) {
        this.eventType = eventType;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getCluster() {
        return cluster;
    }

    public void setCluster(String cluster) {
        this.cluster = cluster;
    }

    public String getBrokerName() {
        return brokerName;
    }

    public void setBrokerName(String brokerName) {
        this.brokerName = brokerName;
    }

    public long getBrokerId() {
        return brokerId;
    }

    public void setBrokerId(long brokerId) {
        this.brokerId = brokerId;
    }

    public String getBrokerAddress() {
        return brokerAddress;
    }

    public void setBrokerAddress(String brokerAddress) {
        this.brokerAddress = brokerAddress;
    }

    public int getPreviousReadQueueNums() {
        return previousReadQueueNums;
    }

    public void setPreviousReadQueueNums(int previousReadQueueNums) {
        this.previousReadQueueNums = previousReadQueueNums;
    }

    public int getCurrentReadQueueNums() {
        return currentReadQueueNums;
    }

    public void setCurrentReadQueueNums(int currentReadQueueNums) {
        this.currentReadQueueNums = currentReadQueueNums;
    }

    public int getPreviousWriteQueueNums() {
        return previousWriteQueueNums;
    }

    public void setPreviousWriteQueueNums(int previousWriteQueueNums) {
        this.previousWriteQueueNums = previousWriteQueueNums;
    }

    public int getCurrentWriteQueueNums() {
        return currentWriteQueueNums;
    }

    public void setCurrentWriteQueueNums(int currentWriteQueueNums) {
        this.currentWriteQueueNums = currentWriteQueueNums;
    }

    public TopicRouteSnapshot getRouteSnapshot() {
        return routeSnapshot;
    }

    public void setRouteSnapshot(TopicRouteSnapshot routeSnapshot) {
        this.routeSnapshot = routeSnapshot;
    }
}