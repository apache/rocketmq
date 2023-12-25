package org.apache.rocketmq.remoting.protocol.header;

import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;

public class InitUserRequestHeader implements CommandCustomHeader {

    private String username;

    public InitUserRequestHeader() {
    }

    public InitUserRequestHeader(String username) {
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
