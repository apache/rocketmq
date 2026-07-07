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

import java.util.List;

/**
 * Topic route snapshot model.
 * Contains the complete route data for a topic at a point in time.
 */
public class TopicRouteSnapshot {
    private String topic;
    private List<BrokerInfo> brokers;
    private List<QueueInfo> queues;

    public TopicRouteSnapshot() {
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public List<BrokerInfo> getBrokers() {
        return brokers;
    }

    public void setBrokers(List<BrokerInfo> brokers) {
        this.brokers = brokers;
    }

    public List<QueueInfo> getQueues() {
        return queues;
    }

    public void setQueues(List<QueueInfo> queues) {
        this.queues = queues;
    }

    /**
     * Broker info in route snapshot.
     */
    public static class BrokerInfo {
        private String cluster;
        private String brokerName;
        private java.util.Map<Long, String> brokerAddrs;

        public BrokerInfo() {
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

        public java.util.Map<Long, String> getBrokerAddrs() {
            return brokerAddrs;
        }

        public void setBrokerAddrs(java.util.Map<Long, String> brokerAddrs) {
            this.brokerAddrs = brokerAddrs;
        }
    }

    /**
     * Queue info in route snapshot.
     */
    public static class QueueInfo {
        private String brokerName;
        private int readQueueNums;
        private int writeQueueNums;
        private int perm;

        public QueueInfo() {
        }

        public String getBrokerName() {
            return brokerName;
        }

        public void setBrokerName(String brokerName) {
            this.brokerName = brokerName;
        }

        public int getReadQueueNums() {
            return readQueueNums;
        }

        public void setReadQueueNums(int readQueueNums) {
            this.readQueueNums = readQueueNums;
        }

        public int getWriteQueueNums() {
            return writeQueueNums;
        }

        public void setWriteQueueNums(int writeQueueNums) {
            this.writeQueueNums = writeQueueNums;
        }

        public int getPerm() {
            return perm;
        }

        public void setPerm(int perm) {
            this.perm = perm;
        }
    }
}