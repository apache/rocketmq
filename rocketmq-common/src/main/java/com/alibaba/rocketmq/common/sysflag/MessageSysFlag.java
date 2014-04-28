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
 * @author shijia.wxr<vintage.wang@gmail.com>
 */
public class MessageSysFlag {
    /**
     * SysFlag
     */
    public final static int CompressedFlag = (0x1 << 0);
    public final static int MultiTagsFlag = (0x1 << 1);

    /**
     * 7 6 5 4 3 2 1 0<br>
     * SysFlag 事务相关，从左属，2与3
     */
    public final static int TransactionNotType = (0x0 << 2);
    public final static int TransactionPreparedType = (0x1 << 2);
    public final static int TransactionCommitType = (0x2 << 2);
    public final static int TransactionRollbackType = (0x3 << 2);


    public static int getTransactionValue(final int flag) {
        return flag & TransactionRollbackType;
    }


    public static int resetTransactionValue(final int flag, final int type) {
        return (flag & (~TransactionRollbackType)) | type;
    }


    public static int clearCompressedFlag(final int flag) {
        return flag & (~CompressedFlag);
    }
}
