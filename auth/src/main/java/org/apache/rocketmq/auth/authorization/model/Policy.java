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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.commons.collections.CollectionUtils;
import org.apache.rocketmq.common.action.Action;
import org.apache.rocketmq.auth.authorization.enums.Decision;
import org.apache.rocketmq.auth.authorization.enums.PolicyType;

public class Policy {

    private PolicyType policyType;

    private List<PolicyEntry> entries;

    public static Policy of(List<Resource> resources, List<Action> actions, Environment environment,
        Decision decision) {
        return of(PolicyType.CUSTOM, resources, actions, environment, decision);
    }

    public static Policy of(PolicyType policyType, List<Resource> resources, List<Action> actions,
        Environment environment,
        Decision decision) {
        Policy policy = new Policy();
        policy.setPolicyType(policyType);
        List<PolicyEntry> entries = resources.stream()
            .map(resource -> PolicyEntry.of(resource, actions, environment, decision))
            .collect(Collectors.toList());
        policy.setEntries(entries);
        return policy;
    }

    public static Policy of(PolicyType type, List<PolicyEntry> entries) {
        Policy policy = new Policy();
        policy.setPolicyType(type);
        policy.setEntries(entries);
        return policy;
    }

    public void updateEntry(List<PolicyEntry> newEntries) {
        if (this.entries == null) {
            this.entries = new ArrayList<>();
        }
        newEntries.forEach(newEntry -> {
            PolicyEntry entry = getEntry(newEntry.getResource());
            if (entry == null) {
                this.entries.add(newEntry);
            } else {
                entry.updateEntry(newEntry.getActions(), newEntry.getEnvironment(), newEntry.getDecision());
            }
        });
    }

    public void deleteEntry(Resource resources) {
        PolicyEntry entry = getEntry(resources);
        if (entry != null) {
            this.entries.remove(entry);
        }
    }

    private PolicyEntry getEntry(Resource resource) {
        if (CollectionUtils.isEmpty(this.entries)) {
            return null;
        }
        for (PolicyEntry entry : this.entries) {
            if (Objects.equals(entry.getResource(), resource)) {
                return entry;
            }
        }
        return null;
    }

    public PolicyType getPolicyType() {
        return policyType;
    }

    public void setPolicyType(PolicyType policyType) {
        this.policyType = policyType;
    }

    public List<PolicyEntry> getEntries() {
        return entries;
    }

    public void setEntries(List<PolicyEntry> entries) {
        this.entries = entries;
    }
}
