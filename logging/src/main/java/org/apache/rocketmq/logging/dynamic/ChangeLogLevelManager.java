package org.apache.rocketmq.logging.dynamic;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Service configuration
 */
public final class ChangeLogLevelManager {
    
    /**
     * Collection of log tuning services
     */
    private static final Map<String, ChangeLogLevelProcessUnit> cllpuMap = new ConcurrentHashMap<String, ChangeLogLevelProcessUnit>();
    
    /**
     * Get service instance based on serverId
     *
     * @param serverId -- service serverId
     * @return -- log adjustment service object
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
