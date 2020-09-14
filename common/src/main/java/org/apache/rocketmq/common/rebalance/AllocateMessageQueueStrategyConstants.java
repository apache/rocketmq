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
package org.apache.rocketmq.common.rebalance;

public class AllocateMessageQueueStrategyConstants {

    public static final String ALLOCATE_MACHINE_ROOM_NEARBY = "MACHINE_ROOM_NEARBY";

    public static final String ALLOCATE_MESSAGE_QUEUE_AVERAGELY = "AVG";

    public static final String ALLOCATE_MESSAGE_QUEUE_AVERAGELY_BY_CIRCLE = "AVG_BY_CIRCLE";

    public static final String ALLOCATE_MESSAGE_QUEUE_BY_CONFIG = "CONFIG";

    public static final String ALLOCATE_MESSAGE_QUEUE_BY_MACHINE_ROOM = "MACHINE_ROOM";

    public static final String ALLOCATE_MESSAGE_QUEUE_CONSISTENT_HASH = "CONSISTENT_HASH";

    public static final String ALLOCATE_MESSAGE_QUEUE_STICKY = "STICKY";
}
