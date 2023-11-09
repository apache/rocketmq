package org.apache.rocketmq.remoting.protocol.header;

import org.apache.rocketmq.common.action.Action;
import org.apache.rocketmq.common.action.RocketMQAction;
import org.apache.rocketmq.common.resource.ResourceType;
import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.protocol.RequestCode;

@RocketMQAction(value = RequestCode.CREATE_ACL, resource = ResourceType.CLUSTER, action = Action.UPDATE)
public class CreateAclRequestHeader implements CommandCustomHeader {

    private String subject;

    public CreateAclRequestHeader() {
    }

    public CreateAclRequestHeader(String subject) {
        this.subject = subject;
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
}
