package org.apache.rocketmq.logging.dynamic;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 服务配置
 */
public final class ChangeLogLevelManager {
    
    /**
     * 日志调整服务集合
     */
    private static final Map<String, ChangeLogLevelProcessUnit> cllpuMap = new ConcurrentHashMap<String, ChangeLogLevelProcessUnit>();
    
    /**
     * 根据serverId获取服务实例
     *
     * @param serverId -- 服务serverId
     * @return -- 日志调整服务对象
     */
    public static ChangeLogLevelProcessUnit getChageLogLevelProcess(String serverId) {
        ChangeLogLevelProcessUnit process = cllpuMap.get(serverId);
        if (null == process) {
            synchronized (cllpuMap) {
                if (null == process) {
                    process = new ChangeLogLevelProcessUnit();
                    cllpuMap.put(serverId, process);
                }
            }
        }
        return process;
    }
}
