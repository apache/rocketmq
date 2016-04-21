package com.alibaba.rocketmq.common.message;

/**
 * Created by alvin on 16-4-20.
 */
public enum MessageType {
    Normal_Msg,
    Trans_Msg_Half,
    Trans_msg_Commit,
    Delay_Msg,
}
