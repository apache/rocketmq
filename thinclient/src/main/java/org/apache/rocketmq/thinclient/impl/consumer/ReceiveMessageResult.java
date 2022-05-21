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

package org.apache.rocketmq.thinclient.impl.consumer;

import apache.rocketmq.v2.Status;
import java.util.List;
import java.util.Optional;
import org.apache.rocketmq.thinclient.message.MessageViewImpl;
import org.apache.rocketmq.thinclient.route.Endpoints;

public class ReceiveMessageResult {
    private final Endpoints endpoints;
    private final Status status;

    private final List<MessageViewImpl> messages;

    public ReceiveMessageResult(Endpoints endpoints, Status status, List<MessageViewImpl> messages) {
        this.endpoints = endpoints;
        this.status = status;
        this.messages = messages;
    }

    public Endpoints getEndpoints() {
        return endpoints;
    }

    public Optional<Status> getStatus() {
        return null == status ? Optional.empty() : Optional.of(status);
    }

    public List<MessageViewImpl> getMessages() {
        return messages;
    }
}
