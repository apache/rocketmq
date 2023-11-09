package org.apache.rocketmq.remoting.protocol.header;

import org.apache.rocketmq.common.action.Action;
import org.apache.rocketmq.common.action.RocketMQAction;
import org.apache.rocketmq.common.resource.ResourceType;
import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.protocol.RequestCode;

@RocketMQAction(value = RequestCode.GET_ACL, resource = ResourceType.CLUSTER, action = Action.GET)
public class GetAclRequestHeader implements CommandCustomHeader {

    private String subject;

    public GetAclRequestHeader() {
    }

    public GetAclRequestHeader(String subject) {
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
