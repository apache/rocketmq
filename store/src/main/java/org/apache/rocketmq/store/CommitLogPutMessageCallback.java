package org.apache.rocketmq.store;

public interface CommitLogPutMessageCallback {
    void callback(PutMessageResult result) ;
}
