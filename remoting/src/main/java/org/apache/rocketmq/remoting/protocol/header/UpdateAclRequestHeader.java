package org.apache.rocketmq.remoting.protocol.header;

import org.apache.rocketmq.common.action.Action;
import org.apache.rocketmq.common.action.RocketMQAction;
import org.apache.rocketmq.common.resource.ResourceType;
import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.protocol.RequestCode;

@RocketMQAction(value = RequestCode.UPDATE_ACL, resource = ResourceType.CLUSTER, action = Action.UPDATE)
public class UpdateAclRequestHeader implements CommandCustomHeader {

    private String subject;

    public UpdateAclRequestHeader() {
    }

    public UpdateAclRequestHeader(String subject) {
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
