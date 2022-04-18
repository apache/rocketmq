package org.apache.rocketmq.common.protocol.header.namesrv.controller;

/**
 * @author hzh
 * @email 642256541@qq.com
 * @date 2022/4/16 20:01
 */
public enum ErrorCodes {

    NONE((short) 0, "No error"),
    FENCED_LEADER_EPOCH((short) 1, "The leader epoch in the request is older than the current epoch"),
    FENCED_SYNC_STATE_SET_EPOCH((short) 2, "The syncStateSet epoch in the request is older than the current epoch"),
    INVALID_REQUEST((short) 3, "The request is invalid"),
    MASTER_NOT_AVAILABLE((short) 4, "There is no available master for this broker.");


    short code;
    String description;

    ErrorCodes(short code, String description) {
        this.code = code;
        this.description = description;
    }

    public short getCode() {
        return code;
    }
}
