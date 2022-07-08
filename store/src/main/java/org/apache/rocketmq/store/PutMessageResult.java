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
package org.apache.rocketmq.store;

public class PutMessageResult {
    private PutMessageStatus putMessageStatus;
    private AppendMessageResult appendMessageResult;
    private boolean remotePut = false;

    public PutMessageResult(PutMessageStatus putMessageStatus, AppendMessageResult appendMessageResult) {
        this.putMessageStatus = putMessageStatus;
        this.appendMessageResult = appendMessageResult;
    }

    public PutMessageResult(PutMessageStatus putMessageStatus, AppendMessageResult appendMessageResult,
        boolean remotePut) {
        this.putMessageStatus = putMessageStatus;
        this.appendMessageResult = appendMessageResult;
        this.remotePut = remotePut;
    }

    public boolean isOk() {
        if (remotePut) {
            return putMessageStatus == PutMessageStatus.PUT_OK || putMessageStatus == PutMessageStatus.FLUSH_DISK_TIMEOUT
                || putMessageStatus == PutMessageStatus.FLUSH_SLAVE_TIMEOUT || putMessageStatus == PutMessageStatus.SLAVE_NOT_AVAILABLE;
        } else {
            return this.appendMessageResult != null && this.appendMessageResult.isOk();
        }

    }

    public AppendMessageResult getAppendMessageResult() {
        return appendMessageResult;
    }

    public void setAppendMessageResult(AppendMessageResult appendMessageResult) {
        this.appendMessageResult = appendMessageResult;
    }

    public PutMessageStatus getPutMessageStatus() {
        return putMessageStatus;
    }

    public void setPutMessageStatus(PutMessageStatus putMessageStatus) {
        this.putMessageStatus = putMessageStatus;
    }

    public boolean isRemotePut() {
        return remotePut;
    }

    public void setRemotePut(boolean remotePut) {
        this.remotePut = remotePut;
    }

    @Override
    public String toString() {
        return "PutMessageResult [putMessageStatus=" + putMessageStatus + ", appendMessageResult="
            + appendMessageResult + ", remotePut=" + remotePut + "]";
    }

}
