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

import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.rocketmq.auth.authentication.manager.AuthenticationMetadataManager;
import org.apache.rocketmq.auth.authentication.model.User;
import org.apache.rocketmq.auth.authorization.manager.AuthorizationMetadataManager;
import org.apache.rocketmq.auth.config.AuthConfig;
import org.apache.rocketmq.auth.migration.v1.PlainPermissionManager;
import org.apache.rocketmq.auth.migration.v1.AclConfig;
import org.apache.rocketmq.auth.migration.v1.PlainAccessConfig;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class AuthMigratorTest {

    @Mock
    private AuthenticationMetadataManager authenticationMetadataManager;

    @Mock
    private AuthorizationMetadataManager authorizationMetadataManager;

    @Mock
    private PlainPermissionManager plainPermissionManager;

    @Mock
    private AuthConfig authConfig;

    private AuthMigrator authMigrator;

    @Before
    public void setUp() throws IllegalAccessException {
        when(authConfig.isMigrateAuthFromV1Enabled()).thenReturn(true);
        authMigrator = new AuthMigrator(authConfig);
        FieldUtils.writeDeclaredField(authMigrator, "authenticationMetadataManager", authenticationMetadataManager, true);
        FieldUtils.writeDeclaredField(authMigrator, "authorizationMetadataManager", authorizationMetadataManager, true);
        FieldUtils.writeDeclaredField(authMigrator, "plainPermissionManager", plainPermissionManager, true);
    }

    @Test
    public void testMigrateNoAclConfigDoesNothing() {
        AclConfig aclConfig = mock(AclConfig.class);
        when(aclConfig.getPlainAccessConfigs()).thenReturn(new ArrayList<>());
        when(plainPermissionManager.getAllAclConfig()).thenReturn(aclConfig);
        authMigrator.migrate();
        verify(authConfig, times(1)).isMigrateAuthFromV1Enabled();
        verify(plainPermissionManager, times(1)).getAllAclConfig();
        verify(authenticationMetadataManager, never()).createUser(any());
        verify(authorizationMetadataManager, never()).createAcl(any());
    }

    @Test
    public void testMigrateWithAclConfigCreatesUserAndAcl() {
        AclConfig aclConfig = mock(AclConfig.class);
        List<PlainAccessConfig> accessConfigs = new ArrayList<>();
        accessConfigs.add(createPlainAccessConfig());
        when(aclConfig.getPlainAccessConfigs()).thenReturn(accessConfigs);
        when(plainPermissionManager.getAllAclConfig()).thenReturn(aclConfig);
        when(authenticationMetadataManager.getUser(anyString()))
                .thenReturn(CompletableFuture.completedFuture(null));
        when(authenticationMetadataManager.createUser(any()))
                .thenReturn(CompletableFuture.completedFuture(null));
        authMigrator.migrate();
        verify(authConfig, times(1)).isMigrateAuthFromV1Enabled();
        verify(plainPermissionManager, times(1)).getAllAclConfig();
        verify(authenticationMetadataManager, times(1)).createUser(any());
        verify(authorizationMetadataManager, times(1)).createAcl(any());
    }

    @Test
    public void testMigrateExceptionInMigrateLogsError() {
        PlainAccessConfig accessConfig = mock(PlainAccessConfig.class);
        when(accessConfig.getAccessKey()).thenReturn("testAk");
        when(authenticationMetadataManager.createUser(any(User.class)))
                .thenThrow(new RuntimeException("Test Exception"));
        AclConfig aclConfig = mock(AclConfig.class);
        List<PlainAccessConfig> accessConfigs = new ArrayList<>();
        accessConfigs.add(accessConfig);
        when(aclConfig.getPlainAccessConfigs()).thenReturn(accessConfigs);
        when(plainPermissionManager.getAllAclConfig()).thenReturn(aclConfig);
        when(authenticationMetadataManager.getUser(anyString()))
                .thenReturn(CompletableFuture.completedFuture(null));
        try {
            authMigrator.migrate();
            verify(authConfig, times(1)).isMigrateAuthFromV1Enabled();
            verify(plainPermissionManager, times(1)).getAllAclConfig();
            verify(authenticationMetadataManager, times(1)).createUser(any());
            verify(authorizationMetadataManager, never()).createAcl(any());
        } catch (final RuntimeException ex) {
            assertEquals("Test Exception", ex.getMessage());
        }
    }

    private PlainAccessConfig createPlainAccessConfig() {
        PlainAccessConfig result = mock(PlainAccessConfig.class);
        when(result.getAccessKey()).thenReturn("testAk");
        when(result.getSecretKey()).thenReturn("testSk");
        when(result.isAdmin()).thenReturn(false);
        when(result.getTopicPerms()).thenReturn(new ArrayList<>());
        when(result.getGroupPerms()).thenReturn(new ArrayList<>());
        when(result.getDefaultTopicPerm()).thenReturn("PUB");
        when(result.getDefaultGroupPerm()).thenReturn(null);
        return result;
    }
}
