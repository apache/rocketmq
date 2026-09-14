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
package org.apache.rocketmq.remoting.metrics;

import io.opentelemetry.api.common.AttributeKey;

public class RemotingMetricsConstant {
    public static final String HISTOGRAM_RPC_LATENCY = "rocketmq_rpc_latency";
    public static final String LABEL_PROTOCOL_TYPE = "protocol_type";
    public static final String LABEL_REQUEST_CODE = "request_code";
    public static final String LABEL_RESPONSE_CODE = "response_code";
    public static final String LABEL_IS_LONG_POLLING = "is_long_polling";
    public static final String LABEL_RESULT = "result";

    // Pre-built typed AttributeKey singletons. Use these in AttributesBuilder.put()
    // on hot paths to avoid allocating a fresh InternalAttributeKeyImpl per call.
    public static final AttributeKey<String>  LABEL_PROTOCOL_TYPE_KEY = AttributeKey.stringKey(LABEL_PROTOCOL_TYPE);
    public static final AttributeKey<String>  LABEL_REQUEST_CODE_KEY = AttributeKey.stringKey(LABEL_REQUEST_CODE);
    public static final AttributeKey<String>  LABEL_RESPONSE_CODE_KEY = AttributeKey.stringKey(LABEL_RESPONSE_CODE);
    public static final AttributeKey<Boolean> LABEL_IS_LONG_POLLING_KEY = AttributeKey.booleanKey(LABEL_IS_LONG_POLLING);
    public static final AttributeKey<String>  LABEL_RESULT_KEY = AttributeKey.stringKey(LABEL_RESULT);

    public static final String PROTOCOL_TYPE_REMOTING = "remoting";

    public static final String RESULT_ONEWAY = "oneway";
    public static final String RESULT_SUCCESS = "success";
    public static final String RESULT_CANCELED = "cancelled";
    public static final String RESULT_PROCESS_REQUEST_FAILED = "process_request_failed";
    public static final String RESULT_WRITE_CHANNEL_FAILED = "write_channel_failed";

}
