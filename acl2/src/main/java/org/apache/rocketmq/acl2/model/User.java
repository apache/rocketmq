package org.apache.rocketmq.acl2.model;

import org.apache.rocketmq.acl2.enums.SubjectType;

public class User implements Subject {

    private String username;

    private String password;

    @Override
    public String getSubjectKey() {
        return this.username;
    }

    @Override
    public SubjectType subjectType() {
        return SubjectType.USER;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }
}
