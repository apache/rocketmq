package org.apache.rocketmq.broker.acl.domain;

import java.util.Iterator;
import java.util.Set;

/**
    *
    *@author ycc
    *@date 2018/08/13
 */

public class AclDomain {
    private String topicId;
    private String groupId;
    private Set<String> userIds;
    private OpsEnum type;
    /**
     * 0 有效
     * -1 无效
     */
    private Integer status;

    public AclDomain(String topicId, String groupId, Set<String> userIds, OpsEnum type, Integer status) {
        this.topicId = topicId;
        this.groupId = groupId;
        this.userIds = userIds;
        this.type = type;
        this.status = status;
    }

    public AclDomain() {
    }

    public String getTopicId() {
        return topicId;
    }

    public void setTopicId(String topicId) {
        this.topicId = topicId;
    }

    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public Set<String> getUserIds() {
        return userIds;
    }

    public void setUserIds(Set<String> userIds) {
        this.userIds = userIds;
    }

    public OpsEnum getType() {
        return type;
    }

    public void setType(OpsEnum type) {
        this.type = type;
    }

    public Integer getStatus() {
        return status;
    }

    public void setStatus(Integer status) {
        this.status = status;
    }

    @Override
    public String toString() {
        if(null!=userIds) {
            Iterator<String> it = userIds.iterator();
            StringBuilder ids = new StringBuilder();
            while(it.hasNext()){
                ids.append(it.next());
                ids.append(",");
            }
            ids.deleteCharAt(ids.length()-1);
            return topicId + "," + groupId + "," + "["+

                    ids+"]" + "," + type.code + "," + status;
        }else{
            return topicId + "," + groupId + "," + userIds + "," + type.code + "," + status;
        }
    }
}
