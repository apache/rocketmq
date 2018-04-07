package org.apache.rocketmq.remoting.protocol;

public interface RemoteCommandResponseCallback {
    void callback(RemotingCommand response);
}
