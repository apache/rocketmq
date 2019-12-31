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
package org.apache.rocketmq.client.trace;

import org.apache.rocketmq.common.MixAll;

public class TraceConstants {

    public static final String GROUP_NAME = "_INNER_TRACE_PRODUCER";
    public static final char CONTENT_SPLITOR = (char) 1;
    public static final char FIELD_SPLITOR = (char) 2;
    public static final String TRACE_INSTANCE_NAME = "PID_CLIENT_INNER_TRACE_PRODUCER";
    public static final String TRACE_TOPIC_PREFIX = MixAll.SYSTEM_TOPIC_PREFIX + "TRACE_DATA_";
}
