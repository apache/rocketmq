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

package org.apache.rocketmq.common.subscription;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class GroupRetryPolicyTest {

    @Test
    public void testGetRetryPolicy() {
        GroupRetryPolicy groupRetryPolicy = new GroupRetryPolicy();
        RetryPolicy retryPolicy = groupRetryPolicy.getRetryPolicy();
        assertThat(retryPolicy).isInstanceOf(CustomizedRetryPolicy.class);
        groupRetryPolicy.setType(GroupRetryPolicyType.EXPONENTIAL);
        retryPolicy = groupRetryPolicy.getRetryPolicy();
        assertThat(retryPolicy).isInstanceOf(CustomizedRetryPolicy.class);

        groupRetryPolicy.setType(GroupRetryPolicyType.CUSTOMIZED);
        groupRetryPolicy.setCustomizedRetryPolicy(new CustomizedRetryPolicy());
        retryPolicy = groupRetryPolicy.getRetryPolicy();
        assertThat(retryPolicy).isInstanceOf(CustomizedRetryPolicy.class);

        groupRetryPolicy.setType(GroupRetryPolicyType.EXPONENTIAL);
        groupRetryPolicy.setExponentialRetryPolicy(new ExponentialRetryPolicy());
        retryPolicy = groupRetryPolicy.getRetryPolicy();
        assertThat(retryPolicy).isInstanceOf(ExponentialRetryPolicy.class);

        groupRetryPolicy.setType(null);
        retryPolicy = groupRetryPolicy.getRetryPolicy();
        assertThat(retryPolicy).isInstanceOf(CustomizedRetryPolicy.class);
    }
}