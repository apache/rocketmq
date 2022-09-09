package org.apache.rocketmq.logging.dynamic;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class MethodInvokerManager {
    
    /**
     * 方法调用服务集合
     */
    private static final Map<String, MethodInvokerProcessUnit> methodMap = new ConcurrentHashMap<String, MethodInvokerProcessUnit>();
    
    /**
     * 根据serverId获取服务实例
     *
     * @param serverId -- 服务serverId
     * @return -- 方法调用服务对象
     */
    public static MethodInvokerProcessUnit getMethodInvokerProcess(String serverId) {
        MethodInvokerProcessUnit process = methodMap.get(serverId);
        if (null == process) {
            synchronized (methodMap) {
                if (null == process) {
                    process = new MethodInvokerProcessUnit();
                    methodMap.put(serverId, process);
                }
            }
        }
        return process;
    }
}
