package org.apache.rocketmq.auth.authentication.model;

import org.apache.rocketmq.auth.authentication.enums.SubjectType;
import org.apache.rocketmq.auth.authentication.enums.UserType;
import org.apache.rocketmq.common.constant.CommonConstants;

public class User implements Subject {

    private String username;

    private String password;

    private UserType userType;

    public static User of(String username) {
        User user = new User();
        user.setUsername(username);
        return user;
    }

    public static User of(String username, String password, UserType userType) {
        User user = new User();
        user.setUsername(username);
        user.setPassword(password);
        user.setUserType(userType);
        return user;
    }

    @Override
    public String toSubjectKey() {
        return this.getSubjectType().getCode() + CommonConstants.COLON + this.username;
    }

    @Override
    public SubjectType getSubjectType() {
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

    public UserType getUserType() {
        return userType;
    }

    public void setUserType(UserType userType) {
        this.userType = userType;
    }
}
