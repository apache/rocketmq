/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.remoting.protocol.subscription;

import com.alibaba.fastjson.annotation.JSONField;
import com.google.common.base.MoreObjects;

public class GroupRetryPolicy {
    private final static RetryPolicy DEFAULT_RETRY_POLICY = new CustomizedRetryPolicy();
    private GroupRetryPolicyType type = GroupRetryPolicyType.CUSTOMIZED;
    private ExponentialRetryPolicy exponentialRetryPolicy;
    private CustomizedRetryPolicy customizedRetryPolicy;

    public GroupRetryPolicyType getType() {
        return type;
    }

    public void setType(GroupRetryPolicyType type) {
        this.type = type;
    }

    public ExponentialRetryPolicy getExponentialRetryPolicy() {
        return exponentialRetryPolicy;
    }

    public void setExponentialRetryPolicy(ExponentialRetryPolicy exponentialRetryPolicy) {
        this.exponentialRetryPolicy = exponentialRetryPolicy;
    }

    public CustomizedRetryPolicy getCustomizedRetryPolicy() {
        return customizedRetryPolicy;
    }

    public void setCustomizedRetryPolicy(CustomizedRetryPolicy customizedRetryPolicy) {
        this.customizedRetryPolicy = customizedRetryPolicy;
    }

    @JSONField(serialize = false, deserialize = false)
    public RetryPolicy getRetryPolicy() {
        if (GroupRetryPolicyType.EXPONENTIAL.equals(type)) {
            if (exponentialRetryPolicy == null) {
                return DEFAULT_RETRY_POLICY;
            }
            return exponentialRetryPolicy;
        } else if (GroupRetryPolicyType.CUSTOMIZED.equals(type)) {
            if (customizedRetryPolicy == null) {
                return DEFAULT_RETRY_POLICY;
            }
            return customizedRetryPolicy;
        } else {
            return DEFAULT_RETRY_POLICY;
        }
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("type", type)
            .add("exponentialRetryPolicy", exponentialRetryPolicy)
            .add("customizedRetryPolicy", customizedRetryPolicy)
            .toString();
    }
}
