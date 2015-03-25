package com.alibaba.rocketmq.filtersrv.filter;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.rocketmq.common.MixAll;
import com.alibaba.rocketmq.common.ThreadFactoryImpl;
import com.alibaba.rocketmq.common.UtilAll;
import com.alibaba.rocketmq.common.constant.LoggerName;
import com.alibaba.rocketmq.common.filter.MessageFilter;
import com.alibaba.rocketmq.filtersrv.FiltersrvController;


public class FilterClassManager {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.FiltersrvLoggerName);

    private ConcurrentHashMap<String/* topic@consumerGroup */, FilterClassInfo> filterClassTable =
            new ConcurrentHashMap<String, FilterClassInfo>(128);

    // 只为编译加锁使用
    private final Object compileLock = new Object();

    private final FiltersrvController filtersrvController;

    // 定时线程
    private final ScheduledExecutorService scheduledExecutorService = Executors
        .newSingleThreadScheduledExecutor(new ThreadFactoryImpl("FSGetClassScheduledThread"));

    private FilterClassFetchMethod filterClassFetchMethod;


    public FilterClassManager(FiltersrvController filtersrvController) {
        this.filtersrvController = filtersrvController;
        this.filterClassFetchMethod =
                new HttpFilterClassFetchMethod(this.filtersrvController.getFiltersrvConfig()
                    .getFilterClassRepertoryUrl());
    }


    public void start() {
        if (!this.filtersrvController.getFiltersrvConfig().isClientUploadFilterClassEnable()) {
            this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

                @Override
                public void run() {
                    fetchClassFromRemoteHost();
                }
            }, 1, 1, TimeUnit.MINUTES);
        }
    }


    public void shutdown() {
        this.scheduledExecutorService.shutdown();
    }


    private void fetchClassFromRemoteHost() {
        Iterator<Entry<String, FilterClassInfo>> it = this.filterClassTable.entrySet().iterator();
        while (it.hasNext()) {
            try {
                Entry<String, FilterClassInfo> next = it.next();
                FilterClassInfo filterClassInfo = next.getValue();
                String[] topicAndGroup = next.getKey().split("@");
                String responseStr =
                        this.filterClassFetchMethod.fetch(topicAndGroup[0], topicAndGroup[1],
                            filterClassInfo.getClassName());
                byte[] filterSourceBinary = responseStr.getBytes("UTF-8");
                int classCRC = UtilAll.crc32(responseStr.getBytes("UTF-8"));
                if (classCRC != filterClassInfo.getClassCRC()) {
                    String javaSource = new String(filterSourceBinary, MixAll.DEFAULT_CHARSET);
                    Class<?> newClass =
                            DynaCode.compileAndLoadClass(filterClassInfo.getClassName(), javaSource);
                    Object newInstance = newClass.newInstance();
                    filterClassInfo.setMessageFilter((MessageFilter) newInstance);
                    filterClassInfo.setClassCRC(classCRC);

                    log.info("fetch Remote class File OK, {} {}", next.getKey(),
                        filterClassInfo.getClassName());
                }
            }
            catch (Exception e) {
                log.error("fetchClassFromRemoteHost Exception", e);
            }
        }
    }


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
            if (this.filtersrvController.getFiltersrvConfig().isClientUploadFilterClassEnable()) {
                if (filterClassInfoPrev.getClassCRC() != classCRC && classCRC != 0) {
                    registerNew = true;
                }
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

                    FilterClassInfo filterClassInfoNew = new FilterClassInfo();
                    filterClassInfoNew.setClassName(className);
                    filterClassInfoNew.setClassCRC(0);
                    filterClassInfoNew.setMessageFilter(null);

                    if (this.filtersrvController.getFiltersrvConfig().isClientUploadFilterClassEnable()) {
                        String javaSource = new String(filterSourceBinary, MixAll.DEFAULT_CHARSET);
                        Class<?> newClass = DynaCode.compileAndLoadClass(className, javaSource);
                        Object newInstance = newClass.newInstance();
                        filterClassInfoNew.setMessageFilter((MessageFilter) newInstance);
                        filterClassInfoNew.setClassCRC(classCRC);
                    }

                    this.filterClassTable.put(key, filterClassInfoNew);
                }
                catch (Throwable e) {
                    String info =
                            String
                                .format(
                                    "FilterServer, registerFilterClass Exception, consumerGroup: %s topic: %s className: %s",
                                    consumerGroup, topic, className);
                    log.error(info, e);
                    return false;
                }
            }
        }

        return true;
    }


    public FilterClassInfo findFilterClass(final String consumerGroup, final String topic) {
        return this.filterClassTable.get(buildKey(consumerGroup, topic));
    }


    public FilterClassFetchMethod getFilterClassFetchMethod() {
        return filterClassFetchMethod;
    }


    public void setFilterClassFetchMethod(FilterClassFetchMethod filterClassFetchMethod) {
        this.filterClassFetchMethod = filterClassFetchMethod;
    }
}
