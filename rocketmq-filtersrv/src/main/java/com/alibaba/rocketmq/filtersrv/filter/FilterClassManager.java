package com.alibaba.rocketmq.filtersrv.filter;

import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.rocketmq.common.MixAll;
import com.alibaba.rocketmq.common.constant.LoggerName;
import com.alibaba.rocketmq.common.filter.MessageFilter;


public class FilterClassManager {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.FiltersrvLoggerName);

    private ConcurrentHashMap<String/* topic@consumerGroup */, FilterClassInfo> filterClassTable =
            new ConcurrentHashMap<String, FilterClassInfo>(128);

    // 只为编译加锁使用
    private final Object compileLock = new Object();


    private static String buildKey(final String consumerGroup, final String topic) {
        return topic + "@" + consumerGroup;
    }


    public boolean registerFilterClass(final String consumerGroup, final String topic,
            final String className, final int classCRC, final byte[] filterSourceBinary) {
        final String key = buildKey(consumerGroup, topic);

        // 先检查是否存在，是否CRC相同
        boolean registerNew = false;
        FilterClassInfo filterClassInfoPrev = this.filterClassTable.get(key);
        if (null == filterClassInfoPrev) {
            registerNew = true;
        }
        else {
            if (filterClassInfoPrev.getClassCRC() != classCRC) {
                registerNew = true;
            }
        }

        // 注册新的Class
        if (registerNew) {
            synchronized (this.compileLock) {
                filterClassInfoPrev = this.filterClassTable.get(key);
                if (null != filterClassInfoPrev && filterClassInfoPrev.getClassCRC() == classCRC) {
                    return true;
                }

                try {
                    String javaSource = new String(filterSourceBinary, MixAll.DEFAULT_CHARSET);
                    Class<?> newClass = DynaCode.compileAndLoadClass(className, javaSource);
                    Object newInstance = newClass.newInstance();
                    FilterClassInfo filterClassInfoNew = new FilterClassInfo();
                    filterClassInfoNew.setClassName(className);
                    filterClassInfoNew.setClassCRC(classCRC);
                    filterClassInfoNew.setMessageFilter((MessageFilter) newInstance);
                    this.filterClassTable.put(key, filterClassInfoNew);
                }
                catch (Throwable e) {
                    log.error("compileAndLoadClass Exception", e);
                    return false;
                }
            }
        }

        return true;
    }


    public FilterClassInfo findFilterClass(final String consumerGroup, final String topic) {
        return this.filterClassTable.get(buildKey(consumerGroup, topic));
    }
}
