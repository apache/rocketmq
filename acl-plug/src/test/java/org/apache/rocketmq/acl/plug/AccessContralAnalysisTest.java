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
package org.apache.rocketmq.acl.plug;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.rocketmq.acl.plug.entity.AccessControl;
import org.apache.rocketmq.acl.plug.entity.BorkerAccessControl;
import org.apache.rocketmq.acl.plug.exception.AclPlugAccountAnalysisException;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class AccessContralAnalysisTest {

    AccessContralAnalysis accessContralAnalysis = new AccessContralAnalysis();

    @Before
    public void init() {
        accessContralAnalysis.analysisClass(RequestCode.class);
    }

    @Test
    public void analysisTest() {
        BorkerAccessControl accessControl = new BorkerAccessControl();
        accessControl.setSendMessage(false);
        Map<Integer, Boolean> map = accessContralAnalysis.analysis(accessControl);

        Iterator<Entry<Integer, Boolean>> it = map.entrySet().iterator();
        long num = 0;
        while (it.hasNext()) {
            Entry<Integer, Boolean> e = it.next();
            if (!e.getValue()) {
                Assert.assertEquals(e.getKey(), Integer.valueOf(10));
                num++;
            }
        }
        Assert.assertEquals(num, 1);
    }

    @Test(expected = AclPlugAccountAnalysisException.class)
    public void analysisExceptionTest() {
        AccessControl accessControl = new AccessControl();
        accessContralAnalysis.analysis(accessControl);
    }

}
