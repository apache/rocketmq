package org.apache.rocketmq.acl2.model;

import java.util.List;
import org.apache.commons.collections.CollectionUtils;
import org.apache.rocketmq.acl2.enums.Action;
import org.apache.rocketmq.acl2.enums.Decision;

public class PolicyEntry {

    private List<Resource> resources;

    private List<Action> actions;

    private Environment environment;

    private Decision decision;

    public boolean isMatchResource(Resource resource) {
        if (CollectionUtils.isEmpty(this.resources)) {
            return false;
        }
        return this.resources.stream()
            .anyMatch(r -> r.isMatch(resource));
    }

    public boolean isMatchAction(Action action) {
        if (CollectionUtils.isEmpty(this.actions)) {
            return false;
        }
        return this.actions.contains(action);
    }

    public boolean isMatchEnvironment(Environment environment) {
        if (this.environment == null) {
            return false;
        }
        return this.environment.isMatch(environment);
    }

    public List<Resource> getResources() {
        return resources;
    }

    public void setResources(List<Resource> resources) {
        this.resources = resources;
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
