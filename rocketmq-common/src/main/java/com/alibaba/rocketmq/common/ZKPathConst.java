/**
 * $Id: ZKPathConst.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.common;

/**
 * ZK常量名
 * 
 * @author vintage.wang@gmail.com  shijia.wxr@taobao.com
 */
public class ZKPathConst {
    /**
     * ZK各个节点常量名字，配置参数相关
     */
    public static final String ZK_NODE_CONF = "conf";
    public static final String ZK_NODE_CONF_BROKERS = "brokers";
    public static final String ZK_NODE_CONF_HOST_MAIN = "main";
    public static final String ZK_NODE_CONF_HOST_TOPICS = "topics";
    public static final String ZK_NODE_CONF_SUB_GROUPS = "subgroups";
    public static final String ZK_NODE_CONF_ORDER_TOPICS = "ordertopics";

    /**
     * ZK各个节点常量名字，运行时相关
     */
    public static final String ZK_NODE_RUNTIME = "runtime";
    public static final String ZK_NODE_RUNTIME_BROKERS = "brokers";
    public static final String ZK_NODE_RUNTIME_TOPICS = "topics";
    public static final String ZK_NODE_RUNTIME_SUB_GROUPS = "subgroups";


    /**
     * /metaq/conf
     */
    public static final String getConfPathInZK(final String root) {
        return root + "/" + ZK_NODE_CONF;
    }


    /**
     * /metaq/conf/brokers
     */
    public static final String getHostConfPathInZK(final String root) {
        return getConfPathInZK(root) + "/" + ZK_NODE_CONF_BROKERS;
    }


    /**
     * /metaq/conf/subgroups
     */
    public static final String getSubGroupPathInZK(final String root) {
        return getConfPathInZK(root) + "/" + ZK_NODE_CONF_SUB_GROUPS;
    }


    /**
     * /metaq/conf/subgroups/GROUP_A
     */
    public static final String getSubGroupPathInZK(final String root, final String subgroup) {
        return getConfPathInZK(root) + "/" + ZK_NODE_CONF_SUB_GROUPS + "/" + subgroup;
    }


    /**
     * /metaq/conf/brokers/172.21.8.10/main
     */
    public static final String getHostConfPathInZK(final String root, final String whichHost) {
        return getHostConfPathInZK(root) + "/" + whichHost + "/" + ZK_NODE_CONF_HOST_MAIN;
    }


    /**
     * /metaq/conf/brokers/172.21.8.10/topics
     */
    public static final String getHostTopicConfPathInZK(final String root, final String whichHost) {
        return getHostConfPathInZK(root) + "/" + whichHost + "/" + ZK_NODE_CONF_HOST_TOPICS;
    }


    /**
     * /metaq/conf/brokers/172.21.8.10/topics
     */
    public static final String getHostTopicConfPathInZK(final String root, final String whichHost,
            final String topic) {
        return getHostConfPathInZK(root) + "/" + whichHost + "/" + ZK_NODE_CONF_HOST_TOPICS + "/" + topic;
    }


    /**
     * /metaq/conf/ordertopics
     */
    public static final String getOrderTopicConfPathInZK(final String root) {
        return getConfPathInZK(root) + "/" + ZK_NODE_CONF_ORDER_TOPICS;
    }


    /**
     * /metaq/conf/ordertopics/TRADE
     */
    public static final String getOrderTopicConfPathInZK(final String root, final String topic) {
        return getConfPathInZK(root) + "/" + ZK_NODE_CONF_ORDER_TOPICS + "/" + topic;
    }
}
