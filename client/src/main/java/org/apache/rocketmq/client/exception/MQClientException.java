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
package org.apache.rocketmq.client.exception;

import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.help.FAQUrl;

public class MQClientException extends Exception {
    private static final long serialVersionUID = -5758410930844185841L;
    private int responseCode;
    private String errorMessage;

    public MQClientException(String errorMessage, Throwable cause) {
        super(FAQUrl.attachDefaultURL(errorMessage), cause);
        this.responseCode = -1;
        this.errorMessage = errorMessage;
    }

    public MQClientException(int responseCode, String errorMessage) {
        super(FAQUrl.attachDefaultURL("CODE: " + UtilAll.responseCode2String(responseCode) + "  DESC: "
            + errorMessage));
        this.responseCode = responseCode;
        this.errorMessage = errorMessage;
    }

    public int getResponseCode() {
        return responseCode;
    }

    public MQClientException setResponseCode(final int responseCode) {
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
