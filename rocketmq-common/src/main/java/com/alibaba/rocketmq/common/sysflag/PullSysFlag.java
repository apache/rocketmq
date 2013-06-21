/**
 * $Id: PullSysFlag.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.common.sysflag;

/**
 * Pull接口用到的flag定义
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 * 
 */
public class PullSysFlag {
    private final static int FLAG_COMMIT_OFFSET = 0x1 << 0;
    private final static int FLAG_SUSPEND = 0x1 << 1;
    private final static int FLAG_SUBSCRIPTION = 0x1 << 2;


    public static int buildSysFlag(final boolean commitOffset, final boolean suspend,
            final boolean subscription) {
        int flag = 0;

        if (commitOffset) {
            flag |= FLAG_COMMIT_OFFSET;
        }

        if (suspend) {
            flag |= FLAG_SUSPEND;
        }

        if (subscription) {
            flag |= FLAG_SUBSCRIPTION;
        }

        return flag;
    }


    public static int clearCommitOffsetFlag(final int sysFlag) {
        return sysFlag & (~FLAG_COMMIT_OFFSET);
    }


    public static boolean hasCommitOffsetFlag(final int sysFlag) {
        return (sysFlag & FLAG_COMMIT_OFFSET) == FLAG_COMMIT_OFFSET;
    }


    public static boolean hasSuspendFlag(final int sysFlag) {
        return (sysFlag & FLAG_SUSPEND) == FLAG_SUSPEND;
    }


    public static boolean hasSubscriptionFlag(final int sysFlag) {
        return (sysFlag & FLAG_SUBSCRIPTION) == FLAG_SUBSCRIPTION;
    }
}
