package com.alibaba.rocketmq.client;

import com.alibaba.rocketmq.common.UtilAll;


/**
 * @author manhong.yqd<jodie.yqd@gmail.com>
 * @since 2013-8-26
 */
public class VirtualEnvUtil {
    public static final String VIRTUAL_APPGROUP_PREFIX = "%%PROJECT_%s%%";


    /**
     * @param origin
     * @param projectGroup
     * @return
     */
    public static String buildWithProjectGroup(String origin, String projectGroup) {
        if (!UtilAll.isBlank(projectGroup)) {
            String prefix = String.format(VIRTUAL_APPGROUP_PREFIX, projectGroup);
            if (!origin.endsWith(prefix)) {
                return origin + prefix;
            } else {
                return origin;
            }
        } else {
            return origin;
        }
    }


    /**
     * @param origin
     * @param projectGroup
     * @return
     */
    public static String clearProjectGroup(String origin, String projectGroup) {
        String prefix = String.format(VIRTUAL_APPGROUP_PREFIX, projectGroup);
        if (!UtilAll.isBlank(prefix) && origin.endsWith(prefix)) {
            return origin.substring(0, origin.lastIndexOf(prefix));
        } else {
            return origin;
        }
    }
}
