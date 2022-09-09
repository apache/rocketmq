package org.apache.rocketmq.logging.dynamic;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 服务工厂
 */
public class ProcessUnitFactory {
    
    private static final Map<String, ProcessUnitFactory> pufMap = new ConcurrentHashMap<String, ProcessUnitFactory>();
    
    /**
     * 服务serverId
     */
    private String serverId = null;
    
    /**
     * 构造服务工厂
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
     * 日志级别动态调整
     */
    public AbstractProcessUnitImpl getChangeLogLevelProcess() {
        return ChangeLogLevelManager.getChageLogLevelProcess(serverId);
    }
    
    /**
     * 方法调用处理单元
     */
    public AbstractProcessUnitImpl getMethodInvokerProcess() {
        return MethodInvokerManager.getMethodInvokerProcess(serverId);
    }
    
}
