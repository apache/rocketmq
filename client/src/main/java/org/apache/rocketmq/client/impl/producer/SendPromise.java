package org.apache.rocketmq.client.impl.producer;

import org.apache.rocketmq.client.producer.SendFuture;
import org.apache.rocketmq.client.producer.SendResult;

public interface SendPromise extends SendFuture {

    SendPromise complete(SendResult result);
    SendPromise report(Throwable cause);

}
