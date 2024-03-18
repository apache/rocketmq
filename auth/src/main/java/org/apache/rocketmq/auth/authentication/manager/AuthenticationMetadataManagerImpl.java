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
package org.apache.rocketmq.auth.authentication.manager;

import com.alibaba.fastjson.JSON;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.acl.common.SessionCredentials;
import org.apache.rocketmq.auth.authentication.enums.UserStatus;
import org.apache.rocketmq.auth.authentication.enums.UserType;
import org.apache.rocketmq.auth.authentication.exception.AuthenticationException;
import org.apache.rocketmq.auth.authentication.factory.AuthenticationFactory;
import org.apache.rocketmq.auth.authentication.model.User;
import org.apache.rocketmq.auth.authentication.provider.AuthenticationMetadataProvider;
import org.apache.rocketmq.auth.authorization.factory.AuthorizationFactory;
import org.apache.rocketmq.auth.authorization.provider.AuthorizationMetadataProvider;
import org.apache.rocketmq.auth.config.AuthConfig;
import org.apache.rocketmq.common.utils.ExceptionUtils;

public class AuthenticationMetadataManagerImpl implements AuthenticationMetadataManager {

    private final AuthenticationMetadataProvider authenticationMetadataProvider;

    private final AuthorizationMetadataProvider authorizationMetadataProvider;

    public AuthenticationMetadataManagerImpl(AuthConfig authConfig) {
        this.authenticationMetadataProvider = AuthenticationFactory.getMetadataProvider(authConfig);
        this.authorizationMetadataProvider = AuthorizationFactory.getMetadataProvider(authConfig);
        this.initUser(authConfig);
    }

    @Override
    public void shutdown() {
        if (this.authenticationMetadataProvider != null) {
            this.authenticationMetadataProvider.shutdown();
        }
        if (this.authorizationMetadataProvider != null) {
            this.authorizationMetadataProvider.shutdown();
        }
    }

    @Override
    public void initUser(AuthConfig authConfig) {
        if (authConfig == null) {
            return;
        }
        if (StringUtils.isNotBlank(authConfig.getInitAuthenticationUser())) {
            try {
                User initUser = JSON.parseObject(authConfig.getInitAuthenticationUser(), User.class);
                initUser.setUserType(UserType.SUPER);
                this.getUser(initUser.getUsername()).thenCompose(user -> {
                    if (user != null) {
                        return CompletableFuture.completedFuture(null);
                    }
                    return this.createUser(initUser);
                }).join();
            } catch (Exception e) {
                throw new AuthenticationException("Init authentication user error.", e);
            }
        }
        if (StringUtils.isNotBlank(authConfig.getInnerClientAuthenticationCredentials())) {
            try {
                SessionCredentials credentials = JSON.parseObject(authConfig.getInnerClientAuthenticationCredentials(), SessionCredentials.class);
                User innerUser = User.of(credentials.getAccessKey(), credentials.getSecretKey(), UserType.SUPER);
                this.getUser(innerUser.getUsername()).thenCompose(user -> {
                    if (user != null) {
                        return CompletableFuture.completedFuture(null);
                    }
                    return this.createUser(innerUser);
                }).join();
            } catch (Exception e) {
                throw new AuthenticationException("Init inner client authentication credentials error", e);
            }
        }
    }

    @Override
    public CompletableFuture<Void> createUser(User user) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        try {
            this.validate(user, true);
            if (user.getUserType() == null) {
                user.setUserType(UserType.NORMAL);
            }
            if (user.getUserStatus() == null) {
                user.setUserStatus(UserStatus.ENABLE);
            }
            result = this.getAuthenticationMetadataProvider().getUser(user.getUsername()).thenCompose(old -> {
                if (old != null) {
                    throw new AuthenticationException("The user is existed");
                }
                return this.getAuthenticationMetadataProvider().createUser(user);
            });
        } catch (Exception e) {
            this.handleException(e, result);
        }
        return result;
    }

    @Override
    public CompletableFuture<Void> updateUser(User user) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        try {
            this.validate(user, false);
            result = this.getAuthenticationMetadataProvider().getUser(user.getUsername()).thenCompose(old -> {
                if (old == null) {
                    throw new AuthenticationException("The user is not exist");
                }
                if (StringUtils.isNotBlank(user.getPassword())) {
                    old.setPassword(user.getPassword());
                }
                if (user.getUserType() != null) {
                    old.setUserType(user.getUserType());
                }
                if (user.getUserStatus() != null) {
                    old.setUserStatus(user.getUserStatus());
                }
                return this.getAuthenticationMetadataProvider().updateUser(old);
            });
        } catch (Exception e) {
            this.handleException(e, result);
        }
        return result;
    }

    @Override
    public CompletableFuture<Void> deleteUser(String username) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        try {
            if (StringUtils.isBlank(username)) {
                throw new AuthenticationException("username can not be blank");
            }
            CompletableFuture<Void> deleteUser = this.getAuthenticationMetadataProvider().deleteUser(username);
            CompletableFuture<Void> deleteAcl = this.getAuthorizationMetadataProvider().deleteAcl(User.of(username));
            return CompletableFuture.allOf(deleteUser, deleteAcl);
        } catch (Exception e) {
            this.handleException(e, result);
        }
        return result;
    }

    @Override
    public CompletableFuture<User> getUser(String username) {
        CompletableFuture<User> result = new CompletableFuture<>();
        try {
            if (StringUtils.isBlank(username)) {
                throw new AuthenticationException("username can not be blank");
            }
            result = this.getAuthenticationMetadataProvider().getUser(username);
        } catch (Exception e) {
            this.handleException(e, result);
        }
        return result;
    }

    @Override
    public CompletableFuture<List<User>> listUser(String filter) {
        CompletableFuture<List<User>> result = new CompletableFuture<>();
        try {
            result = this.getAuthenticationMetadataProvider().listUser(filter);
        } catch (Exception e) {
            this.handleException(e, result);
        }
        return result;
    }

    @Override
    public CompletableFuture<Boolean> isSuperUser(String username) {
        return this.getUser(username).thenApply(user -> {
            if (user == null) {
                throw new AuthenticationException("User:{} is not found", username);
            }
            return user.getUserType() == UserType.SUPER;
        });
    }

    private void validate(User user, boolean isCreate) {
        if (user == null) {
            throw new AuthenticationException("user can not be null");
        }
        if (StringUtils.isBlank(user.getUsername())) {
            throw new AuthenticationException("username can not be blank");
        }
        if (isCreate && StringUtils.isBlank(user.getPassword())) {
            throw new AuthenticationException("password can not be blank");
        }
    }

    private void handleException(Exception e, CompletableFuture<?> result) {
        Throwable throwable = ExceptionUtils.getRealException(e);
        result.completeExceptionally(throwable);
    }

    private AuthorizationMetadataProvider getAuthorizationMetadataProvider() {
        if (authenticationMetadataProvider == null) {
            throw new IllegalStateException("The authenticationMetadataProvider is not configured");
        }
        return authorizationMetadataProvider;
    }

    private AuthenticationMetadataProvider getAuthenticationMetadataProvider() {
        if (authorizationMetadataProvider == null) {
            throw new IllegalStateException("The authorizationMetadataProvider is not configured");
        }
        return authenticationMetadataProvider;
    }
}
