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
package org.apache.rocketmq.broker.metrics;

public class BrokerMetricsConstant {
    public static final String SLS_OTEL_PROJECT_HEADER_KEY = "x-sls-otel-project";
    public static final String SLS_OTEL_INSTANCE_ID_KEY = "x-sls-otel-instance-id";
    public static final String SLS_OTEL_AK_ID_KEY = "x-sls-otel-ak-id";
    public static final String SLS_OTEL_AK_SECRET_KEY = "x-sls-otel-ak-secret";
    public static final String OPEN_TELEMETRY_METER_NAME = "broker-meter";
    public static final String GAUGE_BROKER_PERMISSION = "rocketmq_broker_permission";

    public static final String LABEL_CLUSTER_NAME = "cluster";
    public static final String LABEL_NODE_TYPE = "node_type";
    public static final String BROKER_NODE_TYPE = "broker";
    public static final String LABEL_NODE_ID = "node_id";
    public static final String LABEL_AGGREGATION = "aggregation";
    public static final String AGGREGATION_DELTA = "delta";
}
