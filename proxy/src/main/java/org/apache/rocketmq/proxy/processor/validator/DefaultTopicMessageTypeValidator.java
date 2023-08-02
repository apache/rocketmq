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

package org.apache.rocketmq.proxy.processor.validator;

import org.apache.rocketmq.common.attribute.TopicMessageType;
import org.apache.rocketmq.proxy.common.ProxyException;
import org.apache.rocketmq.proxy.common.ProxyExceptionCode;

public class DefaultTopicMessageTypeValidator implements TopicMessageTypeValidator {

    public void validate(TopicMessageType expectedType, TopicMessageType actualType) {
        if (actualType.equals(TopicMessageType.UNSPECIFIED)
                || !actualType.equals(expectedType) && !expectedType.equals(TopicMessageType.MIXED)) {
            String errorInfo = String.format("TopicMessageType validate failed, the expected type is %s, but actual type is %s", expectedType, actualType);
            throw new ProxyException(ProxyExceptionCode.MESSAGE_PROPERTY_CONFLICT_WITH_TYPE, errorInfo);
        }
    }
}
