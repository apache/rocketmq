package com.alibaba.rocketmq.remoting.protocol;

/**
 * Created by manhong.yqd<jodie.yqd@gmail.com> on 15/4/21.
 */
public enum SerializeType {
    JSON((byte) 0),
    ROCKETMQ((byte) 1);

    SerializeType(byte code) {
        this.code = code;
    }

    private byte code;


    public byte getCode() {
        return code;
    }


    public static SerializeType valueOf(byte code) {
        for (SerializeType serializeType : SerializeType.values()) {
            if (serializeType.getCode() == code) {
                return serializeType;
            }
        }
        return null;
    }
}
