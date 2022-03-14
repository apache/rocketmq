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

package org.apache.rocketmq.common.protocol;

/**
 * 
 * gives the reason for a no permission messaging pulling.
 *
 */
public interface ForbiddenType {

    /**
     * 1=forbidden by broker
     */
    int BROKER_FORBIDDEN               = 1;
    /**
     * 2=forbidden by groupId
     */
    int GROUP_FORBIDDEN                = 2;
    /**
     * 3=forbidden by topic
     */
    int TOPIC_FORBIDDEN                = 3;
    /**
     * 4=forbidden by brocasting mode
     */
    int BROADCASTING_DISABLE_FORBIDDEN = 4;
    /**
     * 5=forbidden for a substription(group with a topic)
     */
    int SUBSCRIPTION_FORBIDDEN         = 5;

}
