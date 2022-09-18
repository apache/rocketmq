package org.apache.rocketmq.logging.dynamic;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * service factory
 */
public class ProcessUnitFactory {
    
    private static final Map<String, ProcessUnitFactory> pufMap = new ConcurrentHashMap<String, ProcessUnitFactory>();
    
    /**
     * service serverId
     */
    private String serverId = null;
    
    /**
     * Construct service factory
     *
     * @param serverId -- 服务serverId
     */
    public ProcessUnitFactory(String serverId) {
        this.serverId = serverId;
    }
    
    /**
     * 获取服务工厂
     *
     * @param serverId -- 服务serverId
     * @return -- 服务工厂
     */
    public static ProcessUnitFactory newInstance(String serverId) {
        ProcessUnitFactory puf = pufMap.get(serverId);
        if (puf == null) {
            puf = new ProcessUnitFactory(serverId);
            pufMap.put(serverId, puf);
        }
        return puf;
    }
    
    /**
     * Dynamic adjustment of log level
     */
    public AbstractProcessUnitImpl getChangeLogLevelProcess() {
        return ChangeLogLevelManager.getChageLogLevelProcess(serverId);
    }
    
    /**
     * method call processing unit
     */
    public AbstractProcessUnitImpl getMethodInvokerProcess() {
        return MethodInvokerManager.getMethodInvokerProcess(serverId);
    }
    
}
