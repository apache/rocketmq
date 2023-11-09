package org.apache.rocketmq.acl2.model;

import java.util.List;

public class Policy {

    private Subject subject;

    private List<PolicyEntry> entries;

    private List<PolicyEntry> defaults;

    public Subject getSubject() {
        return subject;
    }

    public void setSubject(Subject subject) {
        this.subject = subject;
    }

    public List<PolicyEntry> getEntries() {
        return entries;
    }

    public void setEntries(List<PolicyEntry> entries) {
        this.entries = entries;
    }

    public List<PolicyEntry> getDefaults() {
        return defaults;
    }

    public void setDefaults(List<PolicyEntry> defaults) {
        this.defaults = defaults;
    }
}
