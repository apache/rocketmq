/**
 * $Id: MessageSysFlag.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.common.sysflag;

/**
 * @author vintage.wang@gmail.com shijia.wxr@taobao.com
 * 
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
}
