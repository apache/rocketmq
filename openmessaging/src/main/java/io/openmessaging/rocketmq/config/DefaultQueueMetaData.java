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
package io.openmessaging.rocketmq.config;

import io.openmessaging.extension.QueueMetaData;

import java.util.List;

public class DefaultQueueMetaData implements QueueMetaData {

    private String queueName;

    private List<QueueMetaData.Partition> partitions;

    public DefaultQueueMetaData(String queueName, List<QueueMetaData.Partition> partitions) {
        this.queueName = queueName;
        this.partitions = partitions;
    }

    @Override
    public String queueName() {
        return queueName;
    }

    @Override
    public List<QueueMetaData.Partition> partitions() {
        return null;
    }

    public static class DefaultPartition implements Partition {

        public DefaultPartition(int partitionId, String partitonHost) {
            this.partitionId = partitionId;
            this.partitonHost = partitonHost;
        }

        private int partitionId;

        private String partitonHost;

        @Override
        public int partitionId() {
            return partitionId;
        }

        @Override
        public String partitonHost() {
            return partitonHost;
        }
    }
}
