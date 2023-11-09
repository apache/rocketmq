package org.apache.rocketmq.remoting.protocol.header;

import org.apache.rocketmq.common.action.Action;
import org.apache.rocketmq.common.action.RocketMQAction;
import org.apache.rocketmq.common.resource.ResourceType;
import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.protocol.RequestCode;

@RocketMQAction(value = RequestCode.LIST_ACL, resource = ResourceType.CLUSTER, action = Action.GET)
public class ListAclsRequestHeader implements CommandCustomHeader {

    private String subjectFilter;

    private String resourceFilter;

    public ListAclsRequestHeader() {
    }

    public ListAclsRequestHeader(String subjectFilter, String resourceFilter) {
        this.subjectFilter = subjectFilter;
        this.resourceFilter = resourceFilter;
    }

    @Override
    public void checkFields() throws RemotingCommandException {

    }

    public String getSubjectFilter() {
        return subjectFilter;
    }

    public void setSubjectFilter(String subjectFilter) {
        this.subjectFilter = subjectFilter;
    }

    public String getResourceFilter() {
        return resourceFilter;
    }

    public void setResourceFilter(String resourceFilter) {
        this.resourceFilter = resourceFilter;
    }
}
