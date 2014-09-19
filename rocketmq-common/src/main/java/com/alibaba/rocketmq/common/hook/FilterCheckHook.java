package com.alibaba.rocketmq.common.hook;

import java.nio.ByteBuffer;


/**
 * 确认消息是否需要过滤 Hook
 * 
 * @author manhong.yqd<jodie.yqd@gmail.com>
 * @since 2014-3-19
 */
public interface FilterCheckHook {
    public String hookName();


    public boolean isFilterMatched(final boolean isUnitMode, final ByteBuffer byteBuffer);
}
