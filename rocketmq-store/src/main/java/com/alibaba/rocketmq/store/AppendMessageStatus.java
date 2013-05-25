/**
 * $Id: AppendMessageStatus.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.store;

/**
 * 向物理队列写消息返回结果码
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 */
public enum AppendMessageStatus {
    // 成功追加消息
    PUT_OK,
    // 走到文件末尾
    END_OF_FILE,
    // 消息大小超限
    MESSAGE_SIZE_EXCEEDED,
    // 未知错误
    UNKNOWN_ERROR,
}
