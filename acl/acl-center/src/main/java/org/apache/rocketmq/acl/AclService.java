package org.apache.rocketmq.acl;

public interface AclService {

    public boolean aclCheck(String topic, String appName);

    public boolean aclCheck(String topic, String appName,String userInfo);

}
