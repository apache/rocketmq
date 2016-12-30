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

package org.apache.rocketmq.filtersrv.filter;

import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.utils.HttpTinyClient;
import org.apache.rocketmq.common.utils.HttpTinyClient.HttpResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpFilterClassFetchMethod implements FilterClassFetchMethod {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.FILTERSRV_LOGGER_NAME);
    private final String url;

    public HttpFilterClassFetchMethod(String url) {
        this.url = url;
    }

    @Override
    public String fetch(String topic, String consumerGroup, String className) {
        String thisUrl = String.format("%s/%s.java", this.url, className);

        try {
            HttpResult result = HttpTinyClient.httpGet(thisUrl, null, null, "UTF-8", 5000);
            if (200 == result.code) {
                return result.content;
            }
        } catch (Exception e) {
            log.error(
                String.format("call <%s> exception, Topic: %s Group: %s", thisUrl, topic, consumerGroup), e);
        }

        return null;
    }
}
