package com.alibaba.rocketmq.filtersrv.filter;

import java.util.concurrent.ConcurrentHashMap;

import com.alibaba.rocketmq.common.filter.MessageFilter;


public class FilterClassManager {
    private ConcurrentHashMap<String/* className@consumerGroup, 必须含有全包名 */, FilterClassInfo> filterClassTable =
            new ConcurrentHashMap<String, FilterClassInfo>(128);

    private FilterClassLoader filterClassLoader = new FilterClassLoader();


    private static String buildKey(final String consumerGroup, final String className) {
        return className + "@" + consumerGroup;
    }


    public void registerFilterClass(final String consumerGroup, final String className, final int classCRC,
            final byte[] classBinary) throws InstantiationException, IllegalAccessException {
        final String key = buildKey(consumerGroup, className);

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
            Class<?> newClass =
                    filterClassLoader.createNewClass(className, classBinary, 0, classBinary.length);
            Object newInstance = newClass.newInstance();
            FilterClassInfo filterClassInfoNew = new FilterClassInfo();
            filterClassInfoNew.setClassName(className);
            filterClassInfoNew.setClassCRC(classCRC);
            filterClassInfoNew.setMessageFilter((MessageFilter) newInstance);
            this.filterClassTable.put(key, filterClassInfoNew);
        }
    }


    public FilterClassInfo findFilterClass(final String consumerGroup, final String className) {
        return this.filterClassTable.get(buildKey(consumerGroup, className));
    }
}
