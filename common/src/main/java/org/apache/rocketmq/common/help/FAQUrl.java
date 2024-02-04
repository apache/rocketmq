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

    public static final String APPLY_TOPIC_URL = "https://rocketmq.apache.org/docs/bestPractice/06FAQ";

    public static final String NAME_SERVER_ADDR_NOT_EXIST_URL = "https://rocketmq.apache.org/docs/bestPractice/06FAQ";

    public static final String GROUP_NAME_DUPLICATE_URL = "https://rocketmq.apache.org/docs/bestPractice/06FAQ";

    public static final String CLIENT_PARAMETER_CHECK_URL = "https://rocketmq.apache.org/docs/bestPractice/06FAQ";

    public static final String SUBSCRIPTION_GROUP_NOT_EXIST = "https://rocketmq.apache.org/docs/bestPractice/06FAQ";

    public static final String CLIENT_SERVICE_NOT_OK = "https://rocketmq.apache.org/docs/bestPractice/06FAQ";

    // FAQ: No route info of this topic, TopicABC
    public static final String NO_TOPIC_ROUTE_INFO = "https://rocketmq.apache.org/docs/bestPractice/06FAQ";

    public static final String LOAD_JSON_EXCEPTION = "https://rocketmq.apache.org/docs/bestPractice/06FAQ";

    public static final String SAME_GROUP_DIFFERENT_TOPIC = "https://rocketmq.apache.org/docs/bestPractice/06FAQ";

    public static final String MQLIST_NOT_EXIST = "https://rocketmq.apache.org/docs/bestPractice/06FAQ";

    public static final String UNEXPECTED_EXCEPTION_URL = "https://rocketmq.apache.org/docs/bestPractice/06FAQ";

    public static final String SEND_MSG_FAILED = "https://rocketmq.apache.org/docs/bestPractice/06FAQ";

    public static final String UNKNOWN_HOST_EXCEPTION = "https://rocketmq.apache.org/docs/bestPractice/06FAQ";

    private static final String TIP_STRING_BEGIN = "\nSee ";
    private static final String TIP_STRING_END = " for further details.";
    private static final String MORE_INFORMATION = "For more information, please visit the url, ";

    public static String suggestTodo(final String url) {
        StringBuilder sb = new StringBuilder(TIP_STRING_BEGIN.length() + url.length() + TIP_STRING_END.length());
        sb.append(TIP_STRING_BEGIN);
        sb.append(url);
        sb.append(TIP_STRING_END);
        return sb.toString();
    }

    public static String attachDefaultURL(final String errorMessage) {
        if (errorMessage != null) {
            int index = errorMessage.indexOf(TIP_STRING_BEGIN);
            if (-1 == index) {
                StringBuilder sb = new StringBuilder(errorMessage.length() + UNEXPECTED_EXCEPTION_URL.length() + MORE_INFORMATION.length() + 1);
                sb.append(errorMessage);
                sb.append("\n");
                sb.append(MORE_INFORMATION);
                sb.append(UNEXPECTED_EXCEPTION_URL);
                return sb.toString();
            }
        }

        return errorMessage;
    }
}
