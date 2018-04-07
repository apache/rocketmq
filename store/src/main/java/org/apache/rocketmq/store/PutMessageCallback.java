package org.apache.rocketmq.store;

public interface PutMessageCallback {
    void callback(PutMessageResult putMessageResult) ;
}
