package org.apache.rocketmq.auth.authorization.context;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.auth.authentication.model.Subject;
import org.apache.rocketmq.common.action.Action;
import org.apache.rocketmq.auth.authorization.model.Resource;

public class AuthorizationContext {

    private Subject subject;

    private Resource resource;

    private List<Action> actions;

    private String sourceIp;

    private Map<String, Object> extInfo;

    @SuppressWarnings("unchecked")
    public <T> T getExtInfo(String key) {
        if (StringUtils.isBlank(key)) {
            return null;
        }
        if (this.extInfo == null) {
            return null;
        }
        Object value = this.extInfo.get(key);
        if (value == null) {
            return null;
        }
        return (T) value;
    }

    public void setExtInfo(String key, Object value) {
        if (StringUtils.isBlank(key) || value == null) {
            return;
        }
        if (this.extInfo == null) {
            this.extInfo = new HashMap<>();
        }
        this.extInfo.put(key, value);
    }

    public boolean hasExtInfo(String key) {
        Object value = getExtInfo(key);
        return value != null;
    }

    public static AuthorizationContext of(Subject subject, Resource resource, Action action, String sourceIp) {
        AuthorizationContext context = new AuthorizationContext();
        context.setSubject(subject);
        context.setResource(resource);
        context.setActions(Collections.singletonList(action));
        context.setSourceIp(sourceIp);
        return context;
    }

    public static AuthorizationContext of(Subject subject, Resource resource, List<Action> actions, String sourceIp) {
        AuthorizationContext context = new AuthorizationContext();
        context.setSubject(subject);
        context.setResource(resource);
        context.setActions(actions);
        context.setSourceIp(sourceIp);
        return context;
    }

    public String getSubjectKey() {
        return this.subject != null ? this.subject.toSubjectKey() : null;
    }

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

    public List<Action> getActions() {
        return actions;
    }

    public void setActions(List<Action> actions) {
        this.actions = actions;
    }

    public String getSourceIp() {
        return sourceIp;
    }

    public void setSourceIp(String sourceIp) {
        this.sourceIp = sourceIp;
    }

    public Map<String, Object> getExtInfo() {
        return extInfo;
    }

    public void setExtInfo(Map<String, Object> extInfo) {
        this.extInfo = extInfo;
    }
}
