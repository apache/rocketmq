package org.apache.rocketmq.client.producer;

import org.apache.rocketmq.common.message.Message;

public interface RequestCallback {
    void onSuccess(final Message message);

    void onException(final Throwable e);
}
