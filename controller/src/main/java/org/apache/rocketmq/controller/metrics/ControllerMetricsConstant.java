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

package org.apache.rocketmq.controller.metrics;

public class ControllerMetricsConstant {

    public static final String LABEL_ADDRESS = "address";
    public static final String LABEL_GROUP = "group";
    public static final String LABEL_PEER_ID = "peer_id";
    public static final String LABEL_AGGREGATION = "aggregation";
    public static final String AGGREGATION_DELTA = "delta";

    public static final String OPEN_TELEMETRY_METER_NAME = "controller";

    public static final String GAUGE_ROLE = "role";

    // unit: B
    public static final String GAUGE_DLEDGER_DISK_USAGE = "dledger_disk_usage";

    public static final String GAUGE_ACTIVE_BROKER_NUM = "active_broker_num";

    public static final String COUNTER_REQUEST_ERROR_TOTAL = "request_error_total";

    public static final String COUNTER_DLEDGER_ERROR_TOTAL = "dledger_error_total";

    public static final String COUNTER_REQUEST_TOTAL = "request_total";

    public static final String COUNTER_ELECT_TOTAL = "elect_total";

    // unit: us
    public static final String HISTOGRAM_REQUEST_LATENCY = "request_latency";

    // unit: us
    public static final String HISTOGRAM_DLEDGER_OP_LATENCY = "dledger_op_latency";

    public static final String LABEL_CLUSTER_NAME = "cluster";

    public static final String LABEL_BROKER_SET = "broker_set";

    public static final String LABEL_REQUEST_TYPE = "request_type";

    public static final String LABEL_REQUEST_HANDLE_STATUS = "request_handle_status";

    public static final String LABEL_DLEDGER_OPERATION = "dledger_operation";

    public static final String LABEL_ELECTION_RESULT = "election_result";

    public static final String LABEL_DLEDGER_ERROR_TYPE = "dledge_error_type";


}
