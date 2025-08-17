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
package org.apache.rocketmq.common.sync;

public class MetadataChangeInfo {

    private String metadataKey;

    private String metadataValue;

    private long timestamp;

    private ChangeType changeType;


    public enum ChangeType {
        CREATED,
        UPDATED,
        DELETED
    }

    public static MetadataChangeInfo created(String metadataKey, String metadataValue) {
        if (metadataKey == null || metadataValue == null) {
            throw new IllegalArgumentException("Metadata key and value cannot be null");
        }
        MetadataChangeInfo metadataChangeInfo = new MetadataChangeInfo();
        metadataChangeInfo.setMetadataKey(metadataKey);
        metadataChangeInfo.setMetadataValue(metadataValue);
        metadataChangeInfo.setChangeType(ChangeType.CREATED);
        metadataChangeInfo.setTimestamp(System.currentTimeMillis());
        return metadataChangeInfo;
    }


    public static MetadataChangeInfo updated(String metadataKey, String metadataValue) {
        if (metadataKey == null || metadataValue == null) {
            throw new IllegalArgumentException("Metadata key and value cannot be null");
        }
        MetadataChangeInfo metadataChangeInfo = new MetadataChangeInfo();
        metadataChangeInfo.setMetadataKey(metadataKey);
        metadataChangeInfo.setMetadataValue(metadataValue);
        metadataChangeInfo.setChangeType(ChangeType.UPDATED);
        metadataChangeInfo.setTimestamp(System.currentTimeMillis());
        return metadataChangeInfo;
    }

    public static MetadataChangeInfo deleted(String metadataKey) {
        if (metadataKey == null) {
            throw new IllegalArgumentException("Metadata key cannot be null");
        }
        MetadataChangeInfo metadataChangeInfo = new MetadataChangeInfo();
        metadataChangeInfo.setMetadataKey(metadataKey);
        metadataChangeInfo.setChangeType(ChangeType.DELETED);
        metadataChangeInfo.setTimestamp(System.currentTimeMillis());
        return metadataChangeInfo;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public ChangeType getChangeType() {
        return changeType;
    }

    public void setChangeType(ChangeType changeType) {
        this.changeType = changeType;
    }

    public String getMetadataKey() {
        return metadataKey;
    }

    public void setMetadataKey(String metadataKey) {
        this.metadataKey = metadataKey;
    }

    public String getMetadataValue() {
        return metadataValue;
    }

    public void setMetadataValue(String metadataValue) {
        this.metadataValue = metadataValue;
    }

}
