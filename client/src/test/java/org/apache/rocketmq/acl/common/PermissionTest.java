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
package org.apache.rocketmq.acl.common;

import org.junit.Assert;
import org.junit.Test;

public class PermissionTest {

    @Test
    public void fromStringGetPermissionTest() {
        byte perm = Permission.parsePermFromString("PUB");
        Assert.assertEquals(perm, Permission.PUB);

        perm = Permission.parsePermFromString("SUB");
        Assert.assertEquals(perm, Permission.SUB);

        perm = Permission.parsePermFromString("PUB|SUB");
        Assert.assertEquals(perm, Permission.PUB | Permission.SUB);

        perm = Permission.parsePermFromString("SUB|PUB");
        Assert.assertEquals(perm, Permission.PUB | Permission.SUB);

        perm = Permission.parsePermFromString("DENY");
        Assert.assertEquals(perm, Permission.DENY);

        perm = Permission.parsePermFromString("1");
        Assert.assertEquals(perm, Permission.DENY);

        perm = Permission.parsePermFromString(null);
        Assert.assertEquals(perm, Permission.DENY);

    }

    @Test
    public void AclExceptionTest() {
        AclException aclException = new AclException("CAL_SIGNATURE_FAILED",10015);
        AclException aclExceptionWithMessage = new AclException("CAL_SIGNATURE_FAILED",10015,"CAL_SIGNATURE_FAILED Exception");
        Assert.assertEquals(aclException.getCode(),10015);
        Assert.assertEquals(aclExceptionWithMessage.getStatus(),"CAL_SIGNATURE_FAILED");
        aclException.setCode(10016);
        Assert.assertEquals(aclException.getCode(),10016);
        aclException.setStatus("netAddress examine scope Exception netAddress");
        Assert.assertEquals(aclException.getStatus(),"netAddress examine scope Exception netAddress");
    }
}
