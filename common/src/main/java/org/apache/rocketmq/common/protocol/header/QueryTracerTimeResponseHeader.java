package org.apache.rocketmq.common.protocol.header;

import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;

public class QueryTracerTimeResponseHeader implements CommandCustomHeader {

    private String result;

    @Override
    public void checkFields() throws RemotingCommandException {

    }

    public String getResult() {
        return result;
    }

    public void setResult(String result) {
        this.result = result;
    }
}
