package org.apache.rocketmq.auth.authorization.model;

import org.apache.rocketmq.auth.authentication.model.Subject;
import org.apache.rocketmq.common.action.Action;

public class RequestContext {

    private Subject subject;

    private Resource resource;

    private Action action;

    private String sourceIp;

    public Subject getSubject() {
        return subject;
    }

    public void setSubject(Subject subject) {
        this.subject = subject;
    }

    public Resource getResource() {
        return resource;
    }

    public void setResource(Resource resource) {
        this.resource = resource;
    }

    public Action getAction() {
        return action;
    }

    public void setAction(Action action) {
        this.action = action;
    }

    public String getSourceIp() {
        return sourceIp;
    }

    public void setSourceIp(String sourceIp) {
        this.sourceIp = sourceIp;
    }
}
