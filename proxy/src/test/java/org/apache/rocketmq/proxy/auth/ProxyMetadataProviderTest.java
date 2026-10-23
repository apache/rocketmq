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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ProxyMetadataProviderTest {

    @Test
    public void testUnsupportedAuthenticationMetadataOperationsReturnFailedFuture() throws Exception {
        ProxyAuthenticationMetadataProvider provider = new ProxyAuthenticationMetadataProvider();

        assertUnsupported(provider.createUser(null), "createUser");
        assertUnsupported(provider.deleteUser("user"), "deleteUser");
        assertUnsupported(provider.updateUser(null), "updateUser");
        assertUnsupported(provider.listUser(null), "listUser");
    }

    @Test
    public void testUnsupportedAuthorizationMetadataOperationsReturnFailedFuture() throws Exception {
        ProxyAuthorizationMetadataProvider provider = new ProxyAuthorizationMetadataProvider();

        assertUnsupported(provider.createAcl(null), "createAcl");
        assertUnsupported(provider.deleteAcl(null), "deleteAcl");
        assertUnsupported(provider.updateAcl(null), "updateAcl");
        assertUnsupported(provider.listAcl(null, null), "listAcl");
    }

    private static void assertUnsupported(CompletableFuture<?> future, String operation) throws Exception {
        assertNotNull(future);
        try {
            future.get();
            fail("Expected unsupported operation future for " + operation);
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof UnsupportedOperationException);
            assertTrue(e.getCause().getMessage().contains(operation));
        }
    }
}
