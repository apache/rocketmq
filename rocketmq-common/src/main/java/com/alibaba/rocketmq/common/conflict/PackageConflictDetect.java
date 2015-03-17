package com.alibaba.rocketmq.common.conflict;

/**
 * Package conflict detector
 *
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @author von gosling<fengjia10@gmail.com>
 */
public class PackageConflictDetect {
    private static boolean detectEnable = Boolean.parseBoolean(System.getProperty(
            "com.alibaba.rocketmq.packageConflictDetect.enable", "true"));

    public static void detectFastjson() {
        if (detectEnable) {
            final String fastjsonVersion = "1.2.3";
            String version = "0.0.0";
            boolean conflict = false;
            try {
                version = com.alibaba.fastjson.JSON.VERSION;
                int code = version.compareTo(fastjsonVersion);
                if (code < 0) {
                    conflict = true;
                }
            } catch (Throwable e) {
                conflict = true;
            }

            if (conflict) {
                throw new RuntimeException(
                        String
                                .format(
                                        "Your fastjson version is %s, or no fastjson, RocketMQ minimum version required: %s",//
                                        version, fastjsonVersion));
            }
        }
    }
}
