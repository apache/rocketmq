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

package org.apache.rocketmq.proxy.grpc.common;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

public class PollCommandResponseManager {
    private final ConcurrentMap<String, PollCommandResponseFuture> futureTable = new ConcurrentHashMap<>();
    private final AtomicLong commandIdGenerator = new AtomicLong(0);

    public String putResponse(int opaque) {
        String commandId = String.valueOf(commandIdGenerator.incrementAndGet());
        futureTable.put(commandId, new PollCommandResponseFuture(commandId, opaque));
        return commandId;
    }

    public PollCommandResponseFuture getResponse(String commandId) {
        return futureTable.get(commandId);
    }
}
