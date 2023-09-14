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
package org.apache.rocketmq.common;

import org.apache.rocketmq.common.help.FAQUrl;

/**
 *
 * This exception is used for broker hooks only : SendMessageHook, ConsumeMessageHook, RPCHook
 * This exception is not ignored while executing hooks and it means that
 * certain processor should return an immediate error response to the client. The
 * error response code is included in AbortProcessException.  it's naming might
 * be confusing, so feel free to refactor this class. Also when any class implements
 * the 3 hook interface mentioned above we should be careful if we want to throw
 * an AbortProcessException, because it will change the control flow of broker
 * and cause a RemotingCommand return error immediately. So be aware of the side
 * effect before throw AbortProcessException in your implementation.
 *
 */
public class AbortProcessException extends RuntimeException {
    private static final long serialVersionUID = -5728810933841185841L;
    private int responseCode;
    private String errorMessage;

    public AbortProcessException(String errorMessage, Throwable cause) {
        super(FAQUrl.attachDefaultURL(errorMessage), cause);
        this.responseCode = -1;
        this.errorMessage = errorMessage;
    }

    public AbortProcessException(int responseCode, String errorMessage) {
        super(FAQUrl.attachDefaultURL("CODE: " + UtilAll.responseCode2String(responseCode) + "  DESC: "
            + errorMessage));
        this.responseCode = responseCode;
        this.errorMessage = errorMessage;
    }

    public int getResponseCode() {
        return responseCode;
    }

    public AbortProcessException setResponseCode(final int responseCode) {
        this.responseCode = responseCode;
        return this;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(final String errorMessage) {
        this.errorMessage = errorMessage;
    }
}