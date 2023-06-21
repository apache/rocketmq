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
package org.apache.rocketmq.tieredstore.metadata;

public class FileSegmentMetadata {

    public static final int STATUS_NEW = 0;
    public static final int STATUS_SEALED = 1;
    public static final int STATUS_DELETED = 2;

    private int type;
    private String path;
    private long baseOffset;
    private int status;
    private long size;

    private long createTimestamp;
    private long beginTimestamp;
    private long endTimestamp;
    private long sealTimestamp;

    // default constructor is used by fastjson
    public FileSegmentMetadata() {

    }

    public FileSegmentMetadata(String path, long baseOffset, int type) {
        this.path = path;
        this.baseOffset = baseOffset;
        this.type = type;
        this.status = STATUS_NEW;
        this.createTimestamp = System.currentTimeMillis();
    }

    public void markSealed() {
        this.status = STATUS_SEALED;
        this.sealTimestamp = System.currentTimeMillis();
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    public long getBaseOffset() {
        return baseOffset;
    }

    public void setBaseOffset(long baseOffset) {
        this.baseOffset = baseOffset;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public long getSize() {
        return size;
    }

    public void setSize(long size) {
        this.size = size;
    }

    public long getCreateTimestamp() {
        return createTimestamp;
    }

    public void setCreateTimestamp(long createTimestamp) {
        this.createTimestamp = createTimestamp;
    }

    public long getBeginTimestamp() {
        return beginTimestamp;
    }

    public void setBeginTimestamp(long beginTimestamp) {
        this.beginTimestamp = beginTimestamp;
    }

    public long getEndTimestamp() {
        return endTimestamp;
    }

    public void setEndTimestamp(long endTimestamp) {
        this.endTimestamp = endTimestamp;
    }

    public long getSealTimestamp() {
        return sealTimestamp;
    }

    public void setSealTimestamp(long sealTimestamp) {
        this.sealTimestamp = sealTimestamp;
    }
}
