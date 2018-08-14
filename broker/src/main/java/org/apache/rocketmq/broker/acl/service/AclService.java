package org.apache.rocketmq.broker.acl.service;

import java.util.Set;

/**
    *
    *@author ycc
    *@date 2018/08/13
    */

public interface AclService {

    public boolean createAcl(String topicId,String groupId,String userId,String password);

    public boolean addReadAcl(String userId,String password, String topicId,String targetUserId);

    public boolean addWriteAcl(String userId, String password, String topicId,String targetUserId);

    public boolean delReadAcl(String userId, String password,String topicId,String targetUserId);

    public boolean delWriteAcl(String userId,String password, String topicId,String targetUserId);

  //  public boolean updateAcl(String oldTopicId,String newTopicId);

    public boolean delTopic(String userId,String password,String topicId);

    public Set<String> getAllReadUserId(String topicId);

    public Set<String> getAllWriteUserId(String topicId);

    public String getAllOwnerUserId(String topicId);

    public boolean canRead(String topicId,String userId);

    public boolean canWrite(String topicId,String userId);

    public boolean isOwner(String topicId,String userId);

}
