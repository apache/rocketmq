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
package org.apache.rocketmq.auth.migration;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.acl.common.AclConstants;
import org.apache.rocketmq.acl.plain.PlainPermissionManager;
import org.apache.rocketmq.auth.authentication.enums.UserType;
import org.apache.rocketmq.auth.authentication.factory.AuthenticationFactory;
import org.apache.rocketmq.auth.authentication.manager.AuthenticationMetadataManager;
import org.apache.rocketmq.auth.authentication.model.Subject;
import org.apache.rocketmq.auth.authentication.model.User;
import org.apache.rocketmq.auth.authorization.enums.Decision;
import org.apache.rocketmq.auth.authorization.enums.PolicyType;
import org.apache.rocketmq.auth.authorization.factory.AuthorizationFactory;
import org.apache.rocketmq.auth.authorization.manager.AuthorizationMetadataManager;
import org.apache.rocketmq.auth.authorization.model.Acl;
import org.apache.rocketmq.auth.authorization.model.Policy;
import org.apache.rocketmq.auth.authorization.model.PolicyEntry;
import org.apache.rocketmq.auth.authorization.model.Resource;
import org.apache.rocketmq.auth.config.AuthConfig;
import org.apache.rocketmq.common.AclConfig;
import org.apache.rocketmq.common.PlainAccessConfig;
import org.apache.rocketmq.common.action.Action;
import org.apache.rocketmq.common.constant.CommonConstants;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.resource.ResourcePattern;
import org.apache.rocketmq.common.resource.ResourceType;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

public class AuthMigrator {

    protected static final Logger LOG = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);

    private final AuthConfig authConfig;

    private final PlainPermissionManager plainPermissionManager;

    private final AuthenticationMetadataManager authenticationMetadataManager;

    private final AuthorizationMetadataManager authorizationMetadataManager;

    public AuthMigrator(AuthConfig authConfig) {
        this.authConfig = authConfig;
        this.plainPermissionManager = new PlainPermissionManager();
        this.authenticationMetadataManager = AuthenticationFactory.getMetadataManager(authConfig);
        this.authorizationMetadataManager = AuthorizationFactory.getMetadataManager(authConfig);
    }

    public void migrate() {
        if (!authConfig.isMigrateAuthFromV1Enabled()) {
            return;
        }

        AclConfig aclConfig = this.plainPermissionManager.getAllAclConfig();
        List<PlainAccessConfig> accessConfigs = aclConfig.getPlainAccessConfigs();
        if (CollectionUtils.isEmpty(accessConfigs)) {
            return;
        }

        for (PlainAccessConfig accessConfig : accessConfigs) {
            doMigrate(accessConfig);
        }
    }

    private void doMigrate(PlainAccessConfig accessConfig) {
        this.isUserExisted(accessConfig.getAccessKey()).thenCompose(existed -> {
            if (existed) {
                return CompletableFuture.completedFuture(null);
            }
            return createUserAndAcl(accessConfig);
        }).exceptionally(ex -> {
            LOG.error("[ACL MIGRATE] An error occurred while migrating ACL configurations for AccessKey:{}.", accessConfig.getAccessKey(), ex);
            return null;
        }).join();
    }

    private CompletableFuture<Void> createUserAndAcl(PlainAccessConfig accessConfig) {
        return createUser(accessConfig).thenCompose(nil -> createAcl(accessConfig));
    }

    private CompletableFuture<Void> createUser(PlainAccessConfig accessConfig) {
        User user = new User();
        user.setUsername(accessConfig.getAccessKey());
        user.setPassword(accessConfig.getSecretKey());
        if (accessConfig.isAdmin()) {
            user.setUserType(UserType.SUPER);
        } else {
            user.setUserType(UserType.NORMAL);
        }
        return this.authenticationMetadataManager.createUser(user);
    }

    private CompletableFuture<Void> createAcl(PlainAccessConfig config) {
        Subject subject = User.of(config.getAccessKey());
        List<Policy> policies = new ArrayList<>();

        Policy customPolicy = null;
        if (CollectionUtils.isNotEmpty(config.getTopicPerms())) {
            for (String topicPerm : config.getTopicPerms()) {
                String[] temp = StringUtils.split(topicPerm, CommonConstants.EQUAL);
                if (temp.length != 2) {
                    continue;
                }
                String topicName = StringUtils.trim(temp[0]);
                String perm = StringUtils.trim(temp[1]);
                Resource resource = Resource.ofTopic(topicName);
                List<Action> actions = parseActions(perm);
                Decision decision = parseDecision(perm);
                PolicyEntry policyEntry = PolicyEntry.of(resource, actions, null, decision);
                if (customPolicy == null) {
                    customPolicy = Policy.of(PolicyType.CUSTOM, new ArrayList<>());
                }
                customPolicy.getEntries().add(policyEntry);
            }
        }
        if (CollectionUtils.isNotEmpty(config.getGroupPerms())) {
            for (String groupPerm : config.getGroupPerms()) {
                String[] temp = StringUtils.split(groupPerm, CommonConstants.EQUAL);
                if (temp.length != 2) {
                    continue;
                }
                String groupName = StringUtils.trim(temp[0]);
                String perm = StringUtils.trim(temp[1]);
                Resource resource = Resource.ofGroup(groupName);
                List<Action> actions = parseActions(perm);
                Decision decision = parseDecision(perm);
                PolicyEntry policyEntry = PolicyEntry.of(resource, actions, null, decision);
                if (customPolicy == null) {
                    customPolicy = Policy.of(PolicyType.CUSTOM, new ArrayList<>());
                }
                customPolicy.getEntries().add(policyEntry);
            }
        }
        if (customPolicy != null) {
            policies.add(customPolicy);
        }

        Policy defaultPolicy = null;
        if (StringUtils.isNotBlank(config.getDefaultTopicPerm())) {
            String topicPerm = StringUtils.trim(config.getDefaultTopicPerm());
            Resource resource = Resource.of(ResourceType.TOPIC, null, ResourcePattern.ANY);
            List<Action> actions = parseActions(topicPerm);
            Decision decision = parseDecision(topicPerm);
            PolicyEntry policyEntry = PolicyEntry.of(resource, actions, null, decision);
            defaultPolicy = Policy.of(PolicyType.DEFAULT, new ArrayList<>());
            defaultPolicy.getEntries().add(policyEntry);
        }
        if (StringUtils.isNotBlank(config.getDefaultGroupPerm())) {
            String groupPerm = StringUtils.trim(config.getDefaultGroupPerm());
            Resource resource = Resource.of(ResourceType.GROUP, null, ResourcePattern.ANY);
            List<Action> actions = parseActions(groupPerm);
            Decision decision = parseDecision(groupPerm);
            PolicyEntry policyEntry = PolicyEntry.of(resource, actions, null, decision);
            if (defaultPolicy == null) {
                defaultPolicy = Policy.of(PolicyType.DEFAULT, new ArrayList<>());
            }
            defaultPolicy.getEntries().add(policyEntry);
        }
        if (defaultPolicy != null) {
            policies.add(defaultPolicy);
        }

        if (CollectionUtils.isEmpty(policies)) {
            return CompletableFuture.completedFuture(null);
        }

        Acl acl = Acl.of(subject, policies);
        return this.authorizationMetadataManager.createAcl(acl);
    }

    private Decision parseDecision(String str) {
        if (StringUtils.isBlank(str)) {
            return Decision.DENY;
        }
        return StringUtils.equals(str, AclConstants.DENY) ? Decision.DENY : Decision.ALLOW;
    }

    private List<Action> parseActions(String str) {
        List<Action> result = new ArrayList<>();
        if (StringUtils.isBlank(str)) {
            result.add(Action.ALL);
        }
        switch (StringUtils.trim(str)) {
            case AclConstants.PUB:
                result.add(Action.PUB);
                break;
            case AclConstants.SUB:
                result.add(Action.SUB);
                break;
            case AclConstants.PUB_SUB:
            case AclConstants.SUB_PUB:
                result.add(Action.PUB);
                result.add(Action.SUB);
                break;
            case AclConstants.DENY:
                result.add(Action.ALL);
                break;
            default:
                result.add(Action.ALL);
                break;
        }
        return result;
    }

    private CompletableFuture<Boolean> isUserExisted(String username) {
        return this.authenticationMetadataManager.getUser(username).thenApply(Objects::nonNull);
    }
}
