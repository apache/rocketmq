package org.apache.rocketmq.acl.common;

public class Permission {

    public static final byte DENY = 1;
    public static final byte ANY = 1 << 1;
    public static final byte PUB = 1 << 2;
    public static final byte SUB = 1 << 3;

    public boolean checkPermission(byte neededPerm, byte ownedPerm) {
        if ((ownedPerm & DENY) > 0) {
            return false;
        }
        if ((neededPerm & ANY) > 0) {
            return ((ownedPerm & PUB) > 0) || ((ownedPerm & SUB) > 0);
        }
        return (neededPerm & ownedPerm) > 0;
    }

}
