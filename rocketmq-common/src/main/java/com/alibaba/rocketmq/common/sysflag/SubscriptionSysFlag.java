/**
 * Copyright (C) 2010-2013 Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.rocketmq.common.sysflag;

/**
 * subscription 配置标识
 * 
 * @author: manhong.yqd<jodie.yqd@gmail.com>
 * @since: 14-5-28
 */
public class SubscriptionSysFlag {
    // 单元化逻辑 topic 标识
    private final static int FLAG_UNIT = 0x1 << 0;


    public static int buildSysFlag(final boolean unit) {
        int sysFlag = 0;

        if (unit) {
            sysFlag |= FLAG_UNIT;
        }

        return sysFlag;
    }


    public static int setUnitFlag(final int sysFlag) {
        return sysFlag | FLAG_UNIT;
    }


    public static int clearUnitFlag(final int sysFlag) {
        return sysFlag & (~FLAG_UNIT);
    }


    public static boolean hasUnitFlag(final int sysFlag) {
        return (sysFlag & FLAG_UNIT) == FLAG_UNIT;
    }


    public static void main(String[] args) {
        System.out.println(0x1 << 0);
    }
}
