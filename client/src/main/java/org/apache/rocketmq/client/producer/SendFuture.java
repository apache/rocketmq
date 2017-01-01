package org.apache.rocketmq.client.producer;

import java.util.concurrent.Future;

public interface SendFuture extends Future<SendResult> {

    /**
     * Returns the cause resulting in sending error
     */
    Throwable getCause();

    /**
     * Returns {@code true} if this task completed.
     */
    @Override
    boolean isDone();

    SendFuture addCallback(SendCallback callback);
    SendFuture removeCallback(SendCallback callback);

}
