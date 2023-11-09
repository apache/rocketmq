package org.apache.rocketmq.remoting.protocol.header;

import java.util.List;
import org.apache.rocketmq.common.action.Action;
import org.apache.rocketmq.common.action.RocketMQAction;
import org.apache.rocketmq.common.resource.ResourceType;
import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.protocol.RequestCode;

@RocketMQAction(value = RequestCode.DELETE_ACL, resource = ResourceType.CLUSTER, action = Action.UPDATE)
public class DeleteAclRequestHeader implements CommandCustomHeader {

    private String subject;

    private String policyType;

    private List<String> resources;

    public DeleteAclRequestHeader() {
    }

    public DeleteAclRequestHeader(String subject, List<String> resources) {
        this.subject = subject;
        this.resources = resources;
    }

    public DeleteAclRequestHeader(String subject, String policyType, List<String> resources) {
        this.subject = subject;
        this.policyType = policyType;
        this.resources = resources;
    }

    @Override
    public void checkFields() throws RemotingCommandException {

    }

    public String getSubject() {
        return subject;
    }

    public void setSubject(String subject) {
        this.subject = subject;
    }

    public String getPolicyType() {
        return policyType;
    }

    public void setPolicyType(String policyType) {
        this.policyType = policyType;
    }

    public List<String> getResources() {
        return resources;
    }

    public void setResources(List<String> resources) {
        this.resources = resources;
    }
}
