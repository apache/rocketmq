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
package org.apache.rocketmq.client.trace.core.common;

import org.apache.rocketmq.common.MixAll;

public class TrackTraceConstants {
    public static final String NAMESRV_ADDR = "NAMESRV_ADDR";
    public static final String ADDRSRV_URL = "ADDRSRV_URL";
    public static final String INSTANCE_NAME = "InstanceName";
    public static final String ASYNC_BUFFER_SIZE = "AsyncBufferSize";
    public static final String MAX_BATCH_NUM = "MaxBatchNum";
    public static final String WAKE_UP_NUM = "WakeUpNum";
    public static final String MAX_MSG_SIZE = "MaxMsgSize";
    public static final String GROUP_NAME = "_INNER_TRACE_PRODUCER";
    public static final String TRACE_TOPIC = MixAll.RMQ_SYS_TRACK_TRACE_TOPIC;
    public static final char CONTENT_SPLITOR = (char) 1;
    public static final char FIELD_SPLITOR = (char) 2;
    public static final String TRACE_DISPATCHER_TYPE = "DispatcherType";
}
