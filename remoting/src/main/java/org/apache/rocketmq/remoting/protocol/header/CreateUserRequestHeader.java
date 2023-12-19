package org.apache.rocketmq.remoting.protocol.header;

import org.apache.rocketmq.common.action.Action;
import org.apache.rocketmq.common.action.RocketMQAction;
import org.apache.rocketmq.common.resource.ResourceType;
import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.protocol.RequestCode;

@RocketMQAction(value = RequestCode.CREATE_USER, resource = ResourceType.CLUSTER, action = Action.UPDATE)
public class CreateUserRequestHeader implements CommandCustomHeader {

    private String username;

    public CreateUserRequestHeader() {
    }

    public CreateUserRequestHeader(String username) {
        this.username = username;
    }

    @Override
    public void checkFields() throws RemotingCommandException {

    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }
}
