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

import org.apache.rocketmq.auth.authentication.model.Subject;
import org.apache.rocketmq.auth.authorization.model.Acl;
import org.apache.rocketmq.auth.config.AuthConfig;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

public final class AuthorizationDisabledMetadataProvider implements AuthorizationMetadataProvider {
    public final static AuthorizationDisabledMetadataProvider INSTANCE = new AuthorizationDisabledMetadataProvider();

    private AuthorizationDisabledMetadataProvider() {
    }

    @Override
    public void initialize(AuthConfig authConfig, Supplier<?> metadataService) {
    }

    @Override
    public void shutdown() {
    }

    @Override
    public CompletableFuture<Void> createAcl(Acl acl) {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> deleteAcl(Subject subject) {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> updateAcl(Acl acl) {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Acl> getAcl(Subject subject) {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<List<Acl>> listAcl(String subjectFilter, String resourceFilter) {
        return CompletableFuture.completedFuture(null);
    }
}
