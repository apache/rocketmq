package com.alibaba.rocketmq.common.conflict;


import java.net.URL;
import java.util.Enumeration;

/**
 * 用来检测包冲突问题，如果低于某个版本，则要求用户升级
 */
public class PackageConflictDetect {
    private static boolean detectEnable = Boolean.parseBoolean(System.getProperty(
        "com.alibaba.rocketmq.packageConflictDetect.enable", "true"));


    /**
     * fastjson的依赖冲突解决
     */
    public static void detectFastjson() {
        if (detectEnable) {
            final String fastjsonVersion = "1.2.3";
            boolean conflict = false;
            try {
                Enumeration<URL> resources = PackageConflictDetect.class.getClassLoader().getResources("com/alibaba/fastjson/JSON.class");
                while(resources.hasMoreElements()){
                    System.out.println("fastjson --> "+resources.nextElement());
                }
                String version = com.alibaba.fastjson.JSON.VERSION;
                int code = version.compareTo(fastjsonVersion);
                // 说明依赖的版本比要求的版本低
                if (code < 0) {
                    conflict = true;
                }
            }
            catch (Throwable e) {
                conflict = true;
            }

            if (conflict) {
                throw new RuntimeException(String.format(
                    "Your fastjson version is too low, or no fastjson, RocketMQ minimum version required: %s",//
                    fastjsonVersion));
            }
        }
    }
}
