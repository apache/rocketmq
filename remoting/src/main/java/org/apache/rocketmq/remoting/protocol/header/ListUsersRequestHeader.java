package org.apache.rocketmq.remoting.protocol.header;

import org.apache.rocketmq.common.action.Action;
import org.apache.rocketmq.common.action.RocketMQAction;
import org.apache.rocketmq.common.resource.ResourceType;
import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.protocol.RequestCode;

@RocketMQAction(value = RequestCode.LIST_USER, resource = ResourceType.CLUSTER, action = Action.GET)
public class ListUsersRequestHeader implements CommandCustomHeader {

    private String filter;

    public ListUsersRequestHeader() {
    }

    public ListUsersRequestHeader(String filter) {
        this.filter = filter;
    }

    @Override
    public void checkFields() throws RemotingCommandException {

    }

    public String getFilter() {
        return filter;
    }

    public void setFilter(String filter) {
        this.filter = filter;
    }
}
