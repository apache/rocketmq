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
package org.apache.rocketmq.auth.authorization.provider;

import com.google.protobuf.GeneratedMessageV3;
import io.grpc.Metadata;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import org.apache.rocketmq.auth.config.AuthConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

public interface AuthorizationProvider<AuthorizationContext> {

    void initialize(AuthConfig config);

    void initialize(AuthConfig config, Supplier<?> metadataService);

    CompletableFuture<Void> authorize(AuthorizationContext context);

    List<AuthorizationContext> newContexts(Metadata metadata, GeneratedMessageV3 message);

    List<AuthorizationContext> newContexts(RemotingCommand command, String remoteAddr);
}
