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

package org.apache.rocketmq.common.attribute;

/**
 * Subscription model for Lite Topic groups.
 *
 * <ul>
 *   <li>{@link #Shared} — multiple clients in the same group can each
 *      independently subscribe to and pull from the same LMQ.
 *      A given message is delivered to whichever client pops it first.</li>
 *   <li>{@link #Exclusive} — only one client at a time can pull from a
 *      given LMQ within the same group. A new subscriber evicts the previous holder;
 *      an in-flight message is treated as already consumed once.</li>
 * </ul>
 */
public enum LiteSubModel {
    Shared,
    Exclusive
}
