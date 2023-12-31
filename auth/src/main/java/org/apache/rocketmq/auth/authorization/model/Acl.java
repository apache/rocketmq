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

import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.collections.CollectionUtils;
import org.apache.rocketmq.auth.authentication.model.Subject;
import org.apache.rocketmq.auth.authorization.enums.Decision;
import org.apache.rocketmq.auth.authorization.enums.PolicyType;
import org.apache.rocketmq.common.action.Action;

public class Acl {

    private Subject subject;

    private List<Policy> policies;

    public static Acl of(Subject subject, Policy policy) {
        return of(subject, Lists.newArrayList(policy));
    }

    public static Acl of(Subject subject, List<Policy> policies) {
        Acl acl = new Acl();
        acl.setSubject(subject);
        acl.setPolicies(policies);
        return acl;
    }

    public static Acl of(Subject subject, List<Resource> resources, List<Action> actions, Environment environment,
        Decision decision) {
        Acl acl = new Acl();
        acl.setSubject(subject);
        Policy policy = Policy.of(resources, actions, environment, decision);
        acl.setPolicies(Lists.newArrayList(policy));
        return acl;
    }

    public void updatePolicy(Policy policy) {
        this.updatePolicy(Lists.newArrayList(policy));
    }

    public void updatePolicy(List<Policy> policies) {
        if (this.policies == null) {
            this.policies = new ArrayList<>();
        }
        policies.forEach(newPolicy -> {
            Policy oldPolicy = this.getPolicy(newPolicy.getPolicyType());
            if (oldPolicy == null) {
                this.policies.add(newPolicy);
            } else {
                oldPolicy.updateEntry(newPolicy.getEntries());
            }
        });
    }

    public void deletePolicy(PolicyType policyType, List<Resource> resources) {
        Policy policy = getPolicy(policyType);
        if (policy == null) {
            return;
        }
        policy.deleteEntry(resources);
        if (CollectionUtils.isEmpty(policy.getEntries())) {
            this.policies.remove(policy);
        }
    }

    public Policy getPolicy(PolicyType policyType) {
        if (CollectionUtils.isEmpty(this.policies)) {
            return null;
        }
        for (Policy policy : this.policies) {
            if (policy.getPolicyType() == policyType) {
                return policy;
            }
        }
        return null;
    }

    public Subject getSubject() {
        return subject;
    }

    public void setSubject(Subject subject) {
        this.subject = subject;
    }

    public List<Policy> getPolicies() {
        return policies;
    }

    public void setPolicies(List<Policy> policies) {
        this.policies = policies;
    }
}
