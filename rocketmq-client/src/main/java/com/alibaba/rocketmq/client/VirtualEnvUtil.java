package com.alibaba.rocketmq.client;

import com.alibaba.rocketmq.common.UtilALl;


/**
 * 虚拟环境相关 API 封装
 * 
 * @author manhong.yqd<jodie.yqd@gmail.com>
 * @since 2013-8-26
 */
public class VirtualEnvUtil {
    public static final String VIRTUAL_APPGROUP_PREFIX = "^%s#";
    private static final String PROJECT_GROUP_PREFIX_PATTERN = "#[^#]*#";


    /**
     * 添加虚拟运行环境相关的projectGroupPrefix
     * 
     * @param origin
     * @param projectGroup
     * @return
     */
    public static String buildWithProjectGroup(String origin, String projectGroup) {
        if (!UtilALl.isBlank(projectGroup)) {
            String prefix = String.format(VIRTUAL_APPGROUP_PREFIX, projectGroup);
            if (!origin.startsWith(prefix)) {
                return prefix + origin;
            }
            else {
                return origin;
            }
        }
        else {
            return origin;
        }
    }


    /**
     * 清除虚拟运行环境相关的projectGroupPrefix
     * 
     * @param origin
     * @return
     */
    public static String clearProjectGroup(String origin) {
        String prefix = Validators.getGroupWithRegularExpression(origin, PROJECT_GROUP_PREFIX_PATTERN);
        if (!UtilALl.isBlank(prefix) && origin.startsWith(prefix)) {
            return origin.substring(prefix.length());
        }
        else {
            return origin;
        }
    }


    public static void main(String[] args) {
        String str = "#AAA#bbbb";
        System.out.println(Validators.getGroupWithRegularExpression(str, PROJECT_GROUP_PREFIX_PATTERN));
    }
}
