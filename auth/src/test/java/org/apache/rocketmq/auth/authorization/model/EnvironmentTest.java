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
package org.apache.rocketmq.auth.authorization.model;

import java.util.Collections;
import org.junit.Assert;
import org.junit.Test;

public class EnvironmentTest {

    @Test
    public void testConstrainedEnvironmentIsMatch() {
        Environment constrained = Environment.of("192.168.1.0/24");
        Assert.assertNotNull(constrained);

        Assert.assertFalse(constrained.isMatch(null));
        Assert.assertFalse(constrained.isMatch(new Environment()));
        Assert.assertFalse(constrained.isMatch(Environment.of("10.0.0.1")));
        Assert.assertTrue(constrained.isMatch(Environment.of("192.168.1.10")));
    }

    @Test
    public void testUnconstrainedEnvironmentIsMatch() {
        Environment unconstrained = new Environment();
        Assert.assertTrue(unconstrained.isMatch(null));
        Assert.assertTrue(unconstrained.isMatch(Environment.of("192.168.1.10")));

        unconstrained.setSourceIps(Collections.emptyList());
        Assert.assertTrue(unconstrained.isMatch(null));
    }
}
