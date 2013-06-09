package com.alibaba.rocketmq.common.constant;

/**
 * @author shijia.wxr<vintage.wang@gmail.com>
 */
public class PermName {
    public static final int PERM_PRIORITY = 0x1 << 3;
    public static final int PERM_READ = 0x1 << 2;
    public static final int PERM_WRITE = 0x1 << 1;
    public static final int PERM_INHERIT = 0x1 << 0;


    public static boolean isReadable(final int perm) {
        return (perm & PERM_READ) == PERM_READ;
    }


    public static boolean isWriteable(final int perm) {
        return (perm & PERM_WRITE) == PERM_WRITE;
    }


    public static boolean isInherited(final int perm) {
        return (perm & PERM_INHERIT) == PERM_INHERIT;
    }


    public static String perm2String(final int perm) {
        final StringBuffer sb = new StringBuffer("---");
        if (isReadable(perm)) {
            sb.replace(0, 1, "R");
        }

        if (isWriteable(perm)) {
            sb.replace(1, 2, "W");
        }

        if (isInherited(perm)) {
            sb.replace(2, 3, "X");
        }

        return sb.toString();
    }
}
