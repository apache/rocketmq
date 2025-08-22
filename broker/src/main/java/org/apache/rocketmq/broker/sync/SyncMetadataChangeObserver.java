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
package org.apache.rocketmq.broker.sync;

import com.alibaba.fastjson2.JSON;
import org.apache.rocketmq.common.sync.MetadataChangeInfo;
import org.apache.rocketmq.common.sync.MetadataChangeObserver;

public class SyncMetadataChangeObserver implements MetadataChangeObserver {


    private final SyncMessageProducer producer;

    public SyncMetadataChangeObserver(SyncMessageProducer producer) {
        this.producer = producer;
    }

    @Override
    public void onCreated(String targetTopic,String metadataKey, Object newMetadata) {
        this.producer.sendMetadataChange(targetTopic, MetadataChangeInfo.created(
            metadataKey,
            JSON.toJSONString(newMetadata)
        ));
    }

    @Override
    public void onUpdated(String targetTopic, String metadataKey, Object newMetadata) {
        MetadataChangeInfo changeInfo = MetadataChangeInfo.updated(
                metadataKey,
            JSON.toJSONString(newMetadata)
        );
        this.producer.sendMetadataChange(targetTopic, changeInfo);
    }

    @Override
    public void onDeleted(String targetTopic,String metadataKey, Object oldMetadata) {
        this.producer.sendMetadataChange(targetTopic, MetadataChangeInfo.deleted(JSON.toJSONString(oldMetadata)));
    }
}
