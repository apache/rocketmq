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
package org.apache.rocketmq.proxy.service.route;

import org.apache.rocketmq.client.common.ClientErrorCode;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.protocol.ResponseCode;

public class TopicRouteHelper {

    public static boolean isTopicNotExistError(Throwable e) {
        if (e instanceof MQBrokerException) {
            if (((MQBrokerException) e).getResponseCode() == ResponseCode.TOPIC_NOT_EXIST) {
                return true;
            }
        }

        if (e instanceof MQClientException) {
            int code = ((MQClientException) e).getResponseCode();
            if (code == ResponseCode.TOPIC_NOT_EXIST || code == ClientErrorCode.NOT_FOUND_TOPIC_EXCEPTION) {
                return true;
            }

            Throwable cause = e.getCause();
            if (cause instanceof MQClientException) {
                int causeCode = ((MQClientException) cause).getResponseCode();
                return causeCode == ResponseCode.TOPIC_NOT_EXIST || causeCode == ClientErrorCode.NOT_FOUND_TOPIC_EXCEPTION;
            }
        }

        return false;
    }
}
