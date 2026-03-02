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
package org.apache.rocketmq.auth.authorization.manager;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.rocketmq.auth.authentication.model.Subject;
import org.apache.rocketmq.auth.authorization.enums.PolicyType;
import org.apache.rocketmq.auth.authorization.model.Acl;
import org.apache.rocketmq.auth.authorization.model.Resource;

public interface AuthorizationMetadataManager {

    void shutdown();

    CompletableFuture<Void> createAcl(Acl acl);

    CompletableFuture<Void> updateAcl(Acl acl);

    CompletableFuture<Void> deleteAcl(Subject subject);

    CompletableFuture<Void> deleteAcl(Subject subject, PolicyType policyType, Resource resource);

    CompletableFuture<Acl> getAcl(Subject subject);

    CompletableFuture<List<Acl>> listAcl(String subjectFilter, String resourceFilter);
}
