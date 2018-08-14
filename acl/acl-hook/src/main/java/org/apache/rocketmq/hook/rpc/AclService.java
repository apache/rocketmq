package org.apache.rocketmq.hook.rpc;

public interface AclService {

    public boolean aclCheck(String topic, String appName);

    public boolean aclCheck(String topic, String appName,String userInfo);

}