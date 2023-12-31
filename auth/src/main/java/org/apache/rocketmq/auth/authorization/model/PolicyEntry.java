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

import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.collections.CollectionUtils;
import org.apache.rocketmq.common.action.Action;
import org.apache.rocketmq.auth.authorization.enums.Decision;

public class PolicyEntry implements Comparable<PolicyEntry> {

    private Resource resource;

    private List<Action> actions;

    private Environment environment;

    private Decision decision;

    public static PolicyEntry of(Resource resource, List<Action> actions, Environment environment, Decision decision) {
        PolicyEntry policyEntry = new PolicyEntry();
        policyEntry.setResource(resource);
        policyEntry.setActions(actions);
        policyEntry.setEnvironment(environment);
        policyEntry.setDecision(decision);
        return policyEntry;
    }

    public void updateEntry(List<Action> actions, Environment environment,
        Decision decision) {
        this.setActions(actions);
        this.setEnvironment(environment);
        this.setDecision(decision);
    }

    public boolean isMatchResource(Resource resource) {
        return this.resource.isMatch(resource);
    }

    public boolean isMatchAction(List<Action> actions) {
        if (CollectionUtils.isEmpty(this.actions)) {
            return false;
        }
        if (actions.contains(Action.ANY)) {
            return true;
        }
        return actions.stream()
            .anyMatch(action -> this.actions.contains(action)
                || this.actions.contains(Action.ALL));
    }

    public boolean isMatchEnvironment(Environment environment) {
        if (this.environment == null) {
            return true;
        }
        return this.environment.isMatch(environment);
    }

    public String toResourceStr() {
        if (resource == null) {
            return null;
        }
        return resource.toResourceKey();
    }

    public List<String> toActionsStr() {
        if (CollectionUtils.isEmpty(actions)) {
            return null;
        }
        return actions.stream().map(Action::getName)
            .collect(Collectors.toList());
    }

    @Override
    public int compareTo(PolicyEntry o) {
        int compare = this.resource.compareTo(o.getResource());
        if (compare != 0) {
            return compare;
        }
        // the decision deny has higher priority
        return this.decision == Decision.DENY ? 1 : o.decision == Decision.DENY ? -1 : 0;
    }

    public Resource getResource() {
        return resource;
    }

    public void setResource(Resource resource) {
        this.resource = resource;
    }

    public List<Action> getActions() {
        return actions;
    }

    public void setActions(List<Action> actions) {
        this.actions = actions;
    }

    public Environment getEnvironment() {
        return environment;
    }

    public void setEnvironment(Environment environment) {
        this.environment = environment;
    }

    public Decision getDecision() {
        return decision;
    }

    public void setDecision(Decision decision) {
        this.decision = decision;
    }
}
