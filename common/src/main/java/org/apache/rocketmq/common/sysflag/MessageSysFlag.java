/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.common.sysflag;

/**
 * 消息系统标记
 */
public class MessageSysFlag {
    /**
     * 标记位 - 压缩
     */
    public final static int COMPRESSED_FLAG = 0x1;
    public final static int MULTI_TAGS_FLAG = 0x1 << 1;
    /**
     * 事务类型 - 非事务
     */
    public final static int TRANSACTION_NOT_TYPE = 0;
    /**
     * 事务类型 - 事务准备
     */
    public final static int TRANSACTION_PREPARED_TYPE = 0x1 << 2;
    /**
     * 事务类型 - 提交
     */
    public final static int TRANSACTION_COMMIT_TYPE = 0x2 << 2;
    /**
     * 事务类型 - 回滚
     */
    public final static int TRANSACTION_ROLLBACK_TYPE = 0x3 << 2;

    public static int getTransactionValue(final int flag) {
        return flag & TRANSACTION_ROLLBACK_TYPE;
    }

    public static int resetTransactionValue(final int flag, final int type) {
        return (flag & (~TRANSACTION_ROLLBACK_TYPE)) | type;
    }

    public static int clearCompressedFlag(final int flag) {
        return flag & (~COMPRESSED_FLAG);
    }
}
