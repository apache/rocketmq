/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.alibaba.rocketmq.common.help;

/**
 * @author shijia.wxr
 */
public class FAQUrl {

    public static final String APPLY_TOPIC_URL = //
            "http://docs.aliyun.com/cn#/pub/ons/faq/exceptions&topic_not_exist";


    public static final String NAME_SERVER_ADDR_NOT_EXIST_URL = //
            "http://docs.aliyun.com/cn#/pub/ons/faq/exceptions&namesrv_not_exist";


    public static final String GROUP_NAME_DUPLICATE_URL = //
            "http://docs.aliyun.com/cn#/pub/ons/faq/exceptions&group_duplicate";


    public static final String CLIENT_PARAMETER_CHECK_URL = //
            "http://docs.aliyun.com/cn#/pub/ons/faq/exceptions&parameter_check_failed";


    public static final String SUBSCRIPTION_GROUP_NOT_EXIST = //
            "http://docs.aliyun.com/cn#/pub/ons/faq/exceptions&subGroup_not_exist";


    public static final String CLIENT_SERVICE_NOT_OK = //
            "http://docs.aliyun.com/cn#/pub/ons/faq/exceptions&service_not_ok";

    // FAQ: No route info of this topic, TopicABC
    public static final String NO_TOPIC_ROUTE_INFO = //
            "http://docs.aliyun.com/cn#/pub/ons/faq/exceptions&topic_not_exist";


    public static final String LOAD_JSON_EXCEPTION = //
            "http://docs.aliyun.com/cn#/pub/ons/faq/exceptions&load_json_exception";


    public static final String SAME_GROUP_DIFFERENT_TOPIC = //
            "http://docs.aliyun.com/cn#/pub/ons/faq/exceptions&subscription_exception";


    public static final String MQLIST_NOT_EXIST = //
            "http://docs.aliyun.com/cn#/pub/ons/faq/exceptions&queue_not_exist";

    public static final String UNEXPECTED_EXCEPTION_URL = //
            "http://docs.aliyun.com/cn#/pub/ons/faq/exceptions&unexpected_exception";


    public static final String SEND_MSG_FAILED = //
            "http://docs.aliyun.com/cn#/pub/ons/faq/exceptions&send_msg_failed";


    public static final String UNKNOWN_HOST_EXCEPTION = //
            "http://docs.aliyun.com/cn#/pub/ons/faq/exceptions&unknown_host";

    private static final String TipStringBegin = "\nSee ";
    private static final String TipStringEnd = " for further details.";


    public static String suggestTodo(final String url) {
        StringBuilder sb = new StringBuilder();
        sb.append(TipStringBegin);
        sb.append(url);
        sb.append(TipStringEnd);
        return sb.toString();
    }

    public static String attachDefaultURL(final String errorMessage) {
        if (errorMessage != null) {
            int index = errorMessage.indexOf(TipStringBegin);
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
