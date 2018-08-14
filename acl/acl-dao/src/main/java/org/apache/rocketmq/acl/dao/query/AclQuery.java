package org.apache.rocketmq.acl.dao.query;

import lombok.Setter;

import java.io.Serializable;

public class AclQuery implements Serializable {
    private static final long serialVersionUID = -8324968145787319789L;

    private String topic;
    private String appName;
    private String userInfo;

    public String getTopic(){
        return this.topic;
    }
    public void setTopic(String topic){
        this.topic = topic;
    }

    public String getAppName(){
        return this.appName;
    }
    public void setAppName(String appName){
        this.appName = appName;
    }

    public String getUserInfo(){
        return this.userInfo;
    }
    public void setUserInfo(String userInfo){
        this.userInfo = userInfo;
    }

}
