/**
 * $Id: ConsumerOffsetManager.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.broker.offset;

import java.io.File;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.rocketmq.broker.BrokerController;
import com.alibaba.rocketmq.common.MetaMix;


/**
 * Consumer消费进度管理
 * 
 * @author vintage.wang@gmail.com shijia.wxr@taobao.com
 * 
 */
public class ConsumerOffsetManager {
    private static final Logger log = LoggerFactory.getLogger(MetaMix.BrokerLoggerName);

    private static final String TOPIC_GROUP_SEPARATOR = "@";
    private static final String QUEUEID_OFFSET_SEPARATOR = ":";
    private static final String OFFSETS_SEPARATOR = " ";
    private final ConcurrentHashMap<String/* topic@group */, ConcurrentHashMap<Integer, Long>> offsetTable =
            new ConcurrentHashMap<String, ConcurrentHashMap<Integer, Long>>(512);

    private volatile ConcurrentHashMap<String, ConcurrentHashMap<Integer, Long>> offsetTableLastLast;
    private volatile ConcurrentHashMap<String, ConcurrentHashMap<Integer, Long>> offsetTableLast;

    private final BrokerController brokerController;


    public ConsumerOffsetManager(BrokerController brokerController) {
        this.brokerController = brokerController;
    }


    public boolean load() {
        try {
            String fileName = this.brokerController.getBrokerConfig().getConsumerOffsetPath();
            String content = MetaMix.file2String(fileName);
            if (content != null) {
                Properties prop = MetaMix.string2Properties(content);
                if (prop != null) {
                    return this.decode(prop);
                }
            }
        }
        catch (Exception e) {
        }

        return true;
    }


    private boolean decode(final Properties prop) {
        Set<Object> keyset = prop.keySet();
        for (Object object : keyset) {
            String key = object.toString();
            String value = prop.getProperty(key);
            if (value != null) {
                String[] items = value.split(OFFSETS_SEPARATOR);
                if (items != null) {
                    for (String item : items) {
                        String[] pair = item.split(QUEUEID_OFFSET_SEPARATOR);
                        try {
                            int queueId = Integer.parseInt(pair[0]);
                            long offset = Long.parseLong(pair[1]);
                            this.commitOffset(key, queueId, offset);
                        }
                        catch (NumberFormatException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
        }

        return true;
    }


    private static ConcurrentHashMap<String, ConcurrentHashMap<Integer, Long>> cloneOffsetTable(
            final ConcurrentHashMap<String, ConcurrentHashMap<Integer, Long>> input) {
        ConcurrentHashMap<String, ConcurrentHashMap<Integer, Long>> out =
                new ConcurrentHashMap<String, ConcurrentHashMap<Integer, Long>>(input.size());

        for (String topicgroup : input.keySet()) {
            ConcurrentHashMap<Integer, Long> map = input.get(topicgroup);
            if (map != null) {
                ConcurrentHashMap<Integer, Long> mapNew = new ConcurrentHashMap<Integer, Long>(map.size());
                for (Integer queueId : map.keySet()) {
                    Long offset = map.get(queueId);
                    Integer queueIdNew = new Integer(queueId.intValue());
                    Long offsetNew = new Long(offset.longValue());
                    map.put(queueIdNew, offsetNew);
                }

                String topicgroupNew = new String(topicgroup);
                out.put(topicgroupNew, mapNew);
            }
        }

        return out;
    }


    private String encode() {
        StringBuilder result = new StringBuilder();

        for (String topicgroup : this.offsetTable.keySet()) {
            ConcurrentHashMap<Integer, Long> map = this.offsetTable.get(topicgroup);
            if (map != null) {
                StringBuilder sb = new StringBuilder();
                sb.append(topicgroup);
                sb.append("=");
                boolean first = true;
                for (Integer queueId : map.keySet()) {
                    Long offset = map.get(queueId);
                    if (offset != null) {
                        String item = queueId + QUEUEID_OFFSET_SEPARATOR + offset;
                        if (!first) {
                            sb.append(OFFSETS_SEPARATOR);
                        }
                        sb.append(item);
                        first = false;
                    }
                }

                if (!first) {
                    sb.append(IOUtils.LINE_SEPARATOR);
                    result.append(sb.toString());
                }
            }
        }

        return result.toString();
    }


    /**
     * 会有定时线程定时刷盘
     */
    public void flush() {
        String content = this.encode();
        if (content != null && content.length() > 0) {
            String fileName = this.brokerController.getBrokerConfig().getConsumerOffsetPath();
            boolean result = MetaMix.string2File(content, fileName);
            log.info("flush consumer offset, " + fileName + (result ? " OK" : " Failed"));
        }
    }


    /**
     * 会有定时线程定时刷盘
     */
    public void flushHistory() {
        String content = this.encode();
        if (content != null && content.length() > 0) {
            String fileName =
                    this.brokerController.getBrokerConfig().getConsumerOffsetHistoryDir() + File.separator
                            + System.currentTimeMillis();
            boolean result = MetaMix.string2File(content, fileName);
            log.info("flush consumer history offset, " + fileName + (result ? " OK" : " Failed"));
        }
    }


    public long computePullTPS(final String topicgroup) {
        ConcurrentHashMap<Integer, Long> mapLast = this.offsetTableLast.get(topicgroup);
        ConcurrentHashMap<Integer, Long> mapLastLast = this.offsetTableLastLast.get(topicgroup);
        long totalMsgs = 0;
        if (mapLast != null && mapLastLast != null) {
            for (Integer queueIdLast : mapLast.keySet()) {
                Long offsetLast = mapLast.get(queueIdLast);
                Long offsetLastLast = mapLastLast.get(queueIdLast);
                if (offsetLast != null && offsetLastLast != null) {
                    long diff = offsetLast - offsetLastLast;
                    totalMsgs += diff;
                }
            }
        }

        if (0 == totalMsgs)
            return 0;

        double pullTps =
                totalMsgs / this.brokerController.getBrokerConfig().getFlushConsumerOffsetHistoryInterval();
        pullTps *= 1000;

        return Double.valueOf(pullTps).longValue();
    }


    public void recordPullTPS() {
        ConcurrentHashMap<String, ConcurrentHashMap<Integer, Long>> snapshotNow =
                cloneOffsetTable(this.offsetTable);
        this.offsetTableLastLast = this.offsetTableLast;
        this.offsetTableLast = snapshotNow;

        if (this.offsetTableLast != null && this.offsetTableLastLast != null) {
            for (String topicgroupLast : this.offsetTableLast.keySet()) {
                long tps = this.computePullTPS(topicgroupLast);
                log.info(topicgroupLast + " pull tps, " + tps);
            }
        }
    }


    public void commitOffset(final String group, final String topic, final int queueId, final long offset) {
        // topic@group
        String key = topic + TOPIC_GROUP_SEPARATOR + group;
        this.commitOffset(key, queueId, offset);
    }


    public long queryOffset(final String group, final String topic, final int queueId) {
        // topic@group
        String key = topic + TOPIC_GROUP_SEPARATOR + group;
        ConcurrentHashMap<Integer, Long> map = this.offsetTable.get(key);
        if (null != map) {
            Long offset = map.get(queueId);
            if (offset != null)
                return offset;
        }

        return -1;
    }


    private void commitOffset(final String key, final int queueId, final long offset) {
        ConcurrentHashMap<Integer, Long> map = this.offsetTable.get(key);
        if (null == map) {
            map = new ConcurrentHashMap<Integer, Long>(32);
            map.put(queueId, offset);
            this.offsetTable.put(key, map);
        }
        else {
            map.put(queueId, offset);
        }
    }
}
