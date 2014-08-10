package com.alibaba.rocketmq.common.protocol.body;

public enum CMResult {
    CR_SUCCESS,
    CR_LATER,
    CR_ROLLBACK,
    CR_COMMIT,
    CR_THROW_EXCEPTION,
    CR_RETURN_NULL,
}
