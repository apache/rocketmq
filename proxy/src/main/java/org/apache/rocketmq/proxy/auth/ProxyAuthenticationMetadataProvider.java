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
package org.apache.rocketmq.proxy.auth;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import org.apache.rocketmq.auth.authentication.provider.AuthenticationMetadataProvider;
import org.apache.rocketmq.auth.authentication.model.User;
import org.apache.rocketmq.auth.config.AuthConfig;
import org.apache.rocketmq.proxy.service.metadata.MetadataService;

public class ProxyAuthenticationMetadataProvider implements AuthenticationMetadataProvider {

    protected AuthConfig authConfig;
    protected MetadataService metadataService;

    @Override
    public void initialize(AuthConfig authConfig, Supplier<?> metadataService) {
        this.authConfig = authConfig;
        if (metadataService != null) {
            this.metadataService = (MetadataService) metadataService.get();
        }
    }

    @Override
    public void shutdown() {

    }

    @Override
    public CompletableFuture<Void> createUser(User user) {
        return null;
    }

    @Override
    public CompletableFuture<Void> deleteUser(String username) {
        return null;
    }

    @Override
    public CompletableFuture<Void> updateUser(User user) {
        return null;
    }

    @Override
    public CompletableFuture<User> getUser(String username) {
        return this.metadataService.getUser(null, username);
    }

    @Override
    public CompletableFuture<List<User>> listUser(String filter) {
        return null;
    }
}
