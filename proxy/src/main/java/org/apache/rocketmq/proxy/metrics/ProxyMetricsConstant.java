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
package org.apache.rocketmq.proxy.metrics;

public class ProxyMetricsConstant {
    public static final String COUNTER_PROXY_CLIENT_ADMIN_REQUESTS_TOTAL =
        "rocketmq_proxy_client_admin_requests_total";
    public static final String COUNTER_PROXY_CLIENT_READ_MODEL_OPERATIONS_TOTAL =
        "rocketmq_proxy_client_read_model_operations_total";
    public static final String GAUGE_PROXY_UP = "rocketmq_proxy_up";
    public static final String GAUGE_PROXY_CLIENT_TOTAL = "rocketmq_proxy_client_total";
    public static final String GAUGE_PROXY_CLIENT_TYPE_TOTAL = "rocketmq_proxy_client_type_total";
    public static final String GAUGE_PROXY_CLIENT_INDEX_TOTAL = "rocketmq_proxy_client_index_total";
    public static final String HISTOGRAM_PROXY_CLIENT_ADMIN_REQUEST_LATENCY =
        "rocketmq_proxy_client_admin_request_latency";

    public static final String LABEL_CLIENT_TYPE = "client_type";
    public static final String LABEL_INDEX_TYPE = "index_type";
    public static final String LABEL_OPERATION = "operation";
    public static final String LABEL_PROXY_MODE = "proxy_mode";
    public static final String LABEL_RESULT = "result";

    public static final String INDEX_TYPE_GROUP = "group";
    public static final String INDEX_TYPE_TOPIC = "topic";
    public static final String INDEX_TYPE_PROXY_ID = "proxy_id";

    public static final String NODE_TYPE_PROXY = "proxy";
}
