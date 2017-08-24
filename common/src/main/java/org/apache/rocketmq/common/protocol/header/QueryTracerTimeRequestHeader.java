package org.apache.rocketmq.common.protocol.header;

import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;

public class QueryTracerTimeRequestHeader implements CommandCustomHeader {

    private String messageTracerTimeId;

    @Override
    public void checkFields() throws RemotingCommandException {

    }

    public String getMessageTracerTimeId() {
        return messageTracerTimeId;
    }

    public void setMessageTracerTimeId(String messageTracerTimeId) {
        this.messageTracerTimeId = messageTracerTimeId;
    }
}
