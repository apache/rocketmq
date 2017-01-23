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
package org.apache.rocketmq.common.help;

public class FAQUrl {

    public static final String APPLY_TOPIC_URL = //
        "http://rocketmq.apache.org/docs/faq/";

    public static final String NAME_SERVER_ADDR_NOT_EXIST_URL = //
        "http://rocketmq.apache.org/docs/faq/";

    public static final String GROUP_NAME_DUPLICATE_URL = //
        "http://rocketmq.apache.org/docs/faq/";

    public static final String CLIENT_PARAMETER_CHECK_URL = //
        "http://rocketmq.apache.org/docs/faq/";

    public static final String SUBSCRIPTION_GROUP_NOT_EXIST = //
        "http://rocketmq.apache.org/docs/faq/";

    public static final String CLIENT_SERVICE_NOT_OK = //
        "http://rocketmq.apache.org/docs/faq/";

    // FAQ: No route info of this topic, TopicABC
    public static final String NO_TOPIC_ROUTE_INFO = //
        "http://rocketmq.apache.org/docs/faq/";

    public static final String LOAD_JSON_EXCEPTION = //
        "http://rocketmq.apache.org/docs/faq/";

    public static final String SAME_GROUP_DIFFERENT_TOPIC = //
        "http://rocketmq.apache.org/docs/faq/";

    public static final String MQLIST_NOT_EXIST = //
        "http://rocketmq.apache.org/docs/faq/";

    public static final String UNEXPECTED_EXCEPTION_URL = //
        "http://rocketmq.apache.org/docs/faq/";

    public static final String SEND_MSG_FAILED = //
        "http://rocketmq.apache.org/docs/faq/";

    public static final String UNKNOWN_HOST_EXCEPTION = //
        "http://rocketmq.apache.org/docs/faq/";

    private static final String TIP_STRING_BEGIN = "\nSee ";
    private static final String TIP_STRING_END = " for further details.";

    public static String suggestTodo(final String url) {
        StringBuilder sb = new StringBuilder();
        sb.append(TIP_STRING_BEGIN);
        sb.append(url);
        sb.append(TIP_STRING_END);
        return sb.toString();
    }

    public static String attachDefaultURL(final String errorMessage) {
        if (errorMessage != null) {
            int index = errorMessage.indexOf(TIP_STRING_BEGIN);
            if (-1 == index) {
                StringBuilder sb = new StringBuilder();
                sb.append(errorMessage);
                sb.append("\n");
                sb.append("For more information, please visit the url, ");
                sb.append(UNEXPECTED_EXCEPTION_URL);
                return sb.toString();
            }
        }

        return errorMessage;
    }
}
