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

package org.apache.rocketmq.grpc.common;

import apache.rocketmq.v1.Broker;
import apache.rocketmq.v1.Partition;
import apache.rocketmq.v1.Permission;
import apache.rocketmq.v1.Resource;
import java.util.ArrayList;
import java.util.List;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.common.protocol.NamespaceUtil;
import org.apache.rocketmq.common.protocol.route.QueueData;

public class Converter {
    /**
     * Get resource name with namespace
     *
     * @param resource topic/group
     * @return
     */
    public static String getResourceNameWithNamespace(Resource resource) {
        return NamespaceUtil.wrapNamespace(resource.getResourceNamespace(), resource.getName());
    }

    public static List<Partition> generatePartitionList(QueueData queueData, Resource topic, Broker broker) {
        List<Partition> partitionList = new ArrayList<>();

        int readQueue = 0;
        int writeQueue = 0;
        int readAndWriteQueue = 0;
        int totalQueue = Math.max(queueData.getWriteQueueNums(), queueData.getReadQueueNums());
        if (PermName.isWriteable(queueData.getPerm()) && PermName.isReadable(queueData.getPerm())) {
            readAndWriteQueue = Math.min(queueData.getWriteQueueNums(), queueData.getReadQueueNums());
            readQueue = queueData.getReadQueueNums() - readAndWriteQueue;
            writeQueue = queueData.getWriteQueueNums() - readAndWriteQueue;
        } else if (PermName.isWriteable(queueData.getPerm())) {
            writeQueue = queueData.getWriteQueueNums();
        } else if (PermName.isReadable(queueData.getPerm())) {
            readQueue = queueData.getReadQueueNums();
        }

        for (int i = 0; i < totalQueue; i++) {
            Partition.Builder builder = Partition.newBuilder()
                .setBroker(broker)
                .setTopic(topic)
                .setId(i);
            if (i < readQueue) {
                builder.setPermission(Permission.READ);
            } else if (i < writeQueue) {
                builder.setPermission(Permission.WRITE);
            } else {
                builder.setPermission(Permission.READ_WRITE);
            }
            partitionList.add(builder.build());
        }
        return partitionList;
    }
}
