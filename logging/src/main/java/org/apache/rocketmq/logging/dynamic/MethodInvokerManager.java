package org.apache.rocketmq.logging.dynamic;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class MethodInvokerManager {
    
    /**
     * Method call service collection
     */
    private static final Map<String, MethodInvokerProcessUnit> methodMap = new ConcurrentHashMap<String, MethodInvokerProcessUnit>();
    
    /**
     * Get service instance based on serverId
     *
     * @param serverId -- server id
     * @return -- method call service object
     */
    public static MethodInvokerProcessUnit getMethodInvokerProcess(String serverId) {
        MethodInvokerProcessUnit process = methodMap.get(serverId);
        if (null == process) {
            synchronized (methodMap) {
                process = new MethodInvokerProcessUnit();
                methodMap.put(serverId, process);
            }
        }
        return process;
    }
}
