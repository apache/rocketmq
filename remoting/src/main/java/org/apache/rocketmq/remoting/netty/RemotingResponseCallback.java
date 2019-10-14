package org.apache.rocketmq.remoting.netty;

import org.apache.rocketmq.remoting.protocol.RemotingCommand;

public interface RemotingResponseCallback {
    void callback(RemotingCommand response);
}
