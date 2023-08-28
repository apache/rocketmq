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
package org.apache.rocketmq.tieredstore.exception;

public class TieredStoreException extends RuntimeException {

    private TieredStoreErrorCode errorCode;
    private long position = -1;

    private String requestId;

    public TieredStoreException(TieredStoreErrorCode errorCode, String errorMessage) {
        super(errorMessage);
        this.errorCode = errorCode;
    }

    public TieredStoreException(TieredStoreErrorCode errorCode, String errorMessage, String requestId) {
        super(errorMessage);
        this.errorCode = errorCode;
        this.requestId = requestId;
    }

    public TieredStoreErrorCode getErrorCode() {
        return errorCode;
    }

    public void setErrorCode(TieredStoreErrorCode errorCode) {
        this.errorCode = errorCode;
    }

    public long getPosition() {
        return position;
    }

    public void setPosition(long position) {
        this.position = position;
    }

    @Override
    public String toString() {
        String errStr = super.toString();
        if (requestId != null) {
            errStr += " requestId: " + requestId;
        }
        if (position != -1) {
            errStr += ", position: " + position;
        }
        return errStr;
    }
}
