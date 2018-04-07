package org.apache.rocketmq.broker.processor;

import org.apache.rocketmq.remoting.protocol.RemotingCommand;

public interface SendMessageCallback {
    void callback(RemotingCommand response);
}
