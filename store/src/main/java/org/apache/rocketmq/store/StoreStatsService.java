/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.store;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;

public class StoreStatsService extends ServiceThread {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    private static final int FREQUENCY_OF_SAMPLING = 1000;

    private static final int MAX_RECORDS_OF_SAMPLING = 60 * 10;
    private static final String[] PUT_MESSAGE_ENTIRE_TIME_MAX_DESC = new String[] {
        "[<=0ms]", "[0~10ms]", "[10~50ms]", "[50~100ms]", "[100~200ms]", "[200~500ms]", "[500ms~1s]", "[1~2s]", "[2~3s]", "[3~4s]", "[4~5s]", "[5~10s]", "[10s~]",
    };

    //The rule to define buckets
    private static final Map<Integer/*interval step size*/, Integer/*times*/> PUT_MESSAGE_ENTIRE_TIME_BUCKETS = new TreeMap<>();
    //buckets
    private TreeMap<Long/*bucket*/, LongAdder/*times*/> buckets = new TreeMap<>();
    private Map<Long/*bucket*/, LongAdder/*times*/> lastBuckets = new TreeMap<>();

    private static int printTPSInterval = 60 * 1;

    private final LongAdder putMessageFailedTimes = new LongAdder();

    private final ConcurrentMap<String, LongAdder> putMessageTopicTimesTotal =
        new ConcurrentHashMap<>(128);
    private final ConcurrentMap<String, LongAdder> putMessageTopicSizeTotal =
        new ConcurrentHashMap<>(128);

    private final LongAdder getMessageTimesTotalFound = new LongAdder();
    private final LongAdder getMessageTransferedMsgCount = new LongAdder();
    private final LongAdder getMessageTimesTotalMiss = new LongAdder();
    private final LinkedList<CallSnapshot> putTimesList = new LinkedList<CallSnapshot>();

    private final LinkedList<CallSnapshot> getTimesFoundList = new LinkedList<CallSnapshot>();
    private final LinkedList<CallSnapshot> getTimesMissList = new LinkedList<CallSnapshot>();
    private final LinkedList<CallSnapshot> transferedMsgCountList = new LinkedList<CallSnapshot>();
    private volatile LongAdder[] putMessageDistributeTime;
    private volatile LongAdder[] lastPutMessageDistributeTime;
    private long messageStoreBootTimestamp = System.currentTimeMillis();
    private volatile long putMessageEntireTimeMax = 0;
    private volatile long getMessageEntireTimeMax = 0;
    // for putMessageEntireTimeMax
    private ReentrantLock putLock = new ReentrantLock();
    // for getMessageEntireTimeMax
    private ReentrantLock getLock = new ReentrantLock();

    private volatile long dispatchMaxBuffer = 0;

    private ReentrantLock samplingLock = new ReentrantLock();
    private long lastPrintTimestamp = System.currentTimeMillis();

    public StoreStatsService() {
        PUT_MESSAGE_ENTIRE_TIME_BUCKETS.put(1,20);  //0-20
        PUT_MESSAGE_ENTIRE_TIME_BUCKETS.put(2,15);  //20-50
        PUT_MESSAGE_ENTIRE_TIME_BUCKETS.put(5,10);  //50-100
        PUT_MESSAGE_ENTIRE_TIME_BUCKETS.put(10,10);  //100-200
        PUT_MESSAGE_ENTIRE_TIME_BUCKETS.put(50,6);  //200-500
        PUT_MESSAGE_ENTIRE_TIME_BUCKETS.put(100,5);  //500-1000
        PUT_MESSAGE_ENTIRE_TIME_BUCKETS.put(1000,9);  //1s-10s

        this.resetPutMessageTimeBuckets();
        this.resetPutMessageDistributeTime();
    }

    private void resetPutMessageTimeBuckets() {
        TreeMap<Long, LongAdder> nextBuckets = new TreeMap<>();
        AtomicLong index = new AtomicLong(0);
        PUT_MESSAGE_ENTIRE_TIME_BUCKETS.forEach((interval, times) -> {
            for (int i = 0; i < times; i++) {
                nextBuckets.put(index.addAndGet(interval), new LongAdder());
            }
        });
        nextBuckets.put(Long.MAX_VALUE, new LongAdder());

        this.lastBuckets = this.buckets;
        this.buckets = nextBuckets;
    }

    public void incPutMessageEntireTime(long value) {
        Map.Entry<Long, LongAdder> targetBucket = buckets.ceilingEntry(value);
        if (targetBucket != null) {
            targetBucket.getValue().add(1);
        }
    }

    public double findPutMessageEntireTimePX(double px) {
        Map<Long, LongAdder> lastBuckets = this.lastBuckets;
        long start = System.currentTimeMillis();
        double result = 0.0;
        long totalRequest = lastBuckets.values().stream().mapToLong(LongAdder::longValue).sum();
        long pxIndex = (long) (totalRequest * px);
        long passCount = 0;
        List<Long> bucketValue = new ArrayList<>(lastBuckets.keySet());
        for (int i = 0; i < bucketValue.size(); i++) {
            long count = lastBuckets.get(bucketValue.get(i)).longValue();
            if (pxIndex <= passCount + count) {
                long relativeIndex = pxIndex - passCount;
                if (i == 0) {
                    result = count == 0 ? 0 : bucketValue.get(i) * relativeIndex / (double)count;
                } else {
                    long lastBucket = bucketValue.get(i - 1);
                    result = lastBucket + (count == 0 ? 0 : (bucketValue.get(i) - lastBucket) * relativeIndex / (double)count);
                }
                break;
            } else {
                passCount += count;
            }
        }
        log.info("findPutMessageEntireTimePX {}={}ms cost {}ms", px, String.format("%.2f", result), System.currentTimeMillis() - start);
        return result;
    }

    private LongAdder[] resetPutMessageDistributeTime() {
        LongAdder[] next = new LongAdder[13];
        for (int i = 0; i < next.length; i++) {
            next[i] = new LongAdder();
        }

        this.lastPutMessageDistributeTime = this.putMessageDistributeTime;

        this.putMessageDistributeTime = next;

        return lastPutMessageDistributeTime;
    }

    public long getPutMessageEntireTimeMax() {
        return putMessageEntireTimeMax;
    }

    public void setPutMessageEntireTimeMax(long value) {
        this.incPutMessageEntireTime(value);
        final LongAdder[] times = this.putMessageDistributeTime;

        if (null == times)
            return;

        // us
        if (value <= 0) {
            times[0].add(1);
        } else if (value < 10) {
            times[1].add(1);
        } else if (value < 50) {
            times[2].add(1);
        } else if (value < 100) {
            times[3].add(1);
        } else if (value < 200) {
            times[4].add(1);
        } else if (value < 500) {
            times[5].add(1);
        } else if (value < 1000) {
            times[6].add(1);
        }
        // 2s
        else if (value < 2000) {
            times[7].add(1);
        }
        // 3s
        else if (value < 3000) {
            times[8].add(1);
        }
        // 4s
        else if (value < 4000) {
            times[9].add(1);
        }
        // 5s
        else if (value < 5000) {
            times[10].add(1);
        }
        // 10s
        else if (value < 10000) {
            times[11].add(1);
        } else {
            times[12].add(1);
        }

        if (value > this.putMessageEntireTimeMax) {
            this.putLock.lock();
            this.putMessageEntireTimeMax =
                value > this.putMessageEntireTimeMax ? value : this.putMessageEntireTimeMax;
            this.putLock.unlock();
        }
    }

    public long getGetMessageEntireTimeMax() {
        return getMessageEntireTimeMax;
    }

    public void setGetMessageEntireTimeMax(long value) {
        if (value > this.getMessageEntireTimeMax) {
            this.getLock.lock();
            this.getMessageEntireTimeMax =
                value > this.getMessageEntireTimeMax ? value : this.getMessageEntireTimeMax;
            this.getLock.unlock();
        }
    }

    public long getDispatchMaxBuffer() {
        return dispatchMaxBuffer;
    }

    public void setDispatchMaxBuffer(long value) {
        this.dispatchMaxBuffer = value > this.dispatchMaxBuffer ? value : this.dispatchMaxBuffer;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder(1024);
        Long totalTimes = getPutMessageTimesTotal();
        if (0 == totalTimes) {
            totalTimes = 1L;
        }

        sb.append("\truntime: " + this.getFormatRuntime() + "\r\n");
        sb.append("\tputMessageEntireTimeMax: " + this.putMessageEntireTimeMax + "\r\n");
        sb.append("\tputMessageTimesTotal: " + totalTimes + "\r\n");
        sb.append("\tgetPutMessageFailedTimes: " + this.getPutMessageFailedTimes() + "\r\n");
        sb.append("\tputMessageSizeTotal: " + this.getPutMessageSizeTotal() + "\r\n");
        sb.append("\tputMessageDistributeTime: " + this.getPutMessageDistributeTimeStringInfo(totalTimes)
            + "\r\n");
        sb.append("\tputMessageAverageSize: " + (this.getPutMessageSizeTotal() / totalTimes.doubleValue())
            + "\r\n");
        sb.append("\tdispatchMaxBuffer: " + this.dispatchMaxBuffer + "\r\n");
        sb.append("\tgetMessageEntireTimeMax: " + this.getMessageEntireTimeMax + "\r\n");
        sb.append("\tputTps: " + this.getPutTps() + "\r\n");
        sb.append("\tgetFoundTps: " + this.getGetFoundTps() + "\r\n");
        sb.append("\tgetMissTps: " + this.getGetMissTps() + "\r\n");
        sb.append("\tgetTotalTps: " + this.getGetTotalTps() + "\r\n");
        sb.append("\tgetTransferedTps: " + this.getGetTransferedTps() + "\r\n");
        return sb.toString();
    }

    public long getPutMessageTimesTotal() {
        Map<String, LongAdder> map = putMessageTopicTimesTotal;
        return map.values()
                .parallelStream()
                .mapToLong(LongAdder::longValue)
                .sum();
    }

    private String getFormatRuntime() {
        final long millisecond = 1;
        final long second = 1000 * millisecond;
        final long minute = 60 * second;
        final long hour = 60 * minute;
        final long day = 24 * hour;
        final MessageFormat messageFormat = new MessageFormat("[ {0} days, {1} hours, {2} minutes, {3} seconds ]");

        long time = System.currentTimeMillis() - this.messageStoreBootTimestamp;
        long days = time / day;
        long hours = (time % day) / hour;
        long minutes = (time % hour) / minute;
        long seconds = (time % minute) / second;
        return messageFormat.format(new Long[] {days, hours, minutes, seconds});
    }

    public long getPutMessageSizeTotal() {
        Map<String, LongAdder> map = putMessageTopicSizeTotal;
        return map.values()
                .parallelStream()
                .mapToLong(LongAdder::longValue)
                .sum();
    }

    private String getPutMessageDistributeTimeStringInfo(Long total) {
        return this.putMessageDistributeTimeToString();
    }

    private String getPutTps() {
        StringBuilder sb = new StringBuilder();

        sb.append(this.getPutTps(10));
        sb.append(" ");

        sb.append(this.getPutTps(60));
        sb.append(" ");

        sb.append(this.getPutTps(600));

        return sb.toString();
    }

    private String getGetFoundTps() {
        StringBuilder sb = new StringBuilder();

        sb.append(this.getGetFoundTps(10));
        sb.append(" ");

        sb.append(this.getGetFoundTps(60));
        sb.append(" ");

        sb.append(this.getGetFoundTps(600));

        return sb.toString();
    }

    private String getGetMissTps() {
        StringBuilder sb = new StringBuilder();

        sb.append(this.getGetMissTps(10));
        sb.append(" ");

        sb.append(this.getGetMissTps(60));
        sb.append(" ");

        sb.append(this.getGetMissTps(600));

        return sb.toString();
    }

    private String getGetTotalTps() {
        StringBuilder sb = new StringBuilder();

        sb.append(this.getGetTotalTps(10));
        sb.append(" ");

        sb.append(this.getGetTotalTps(60));
        sb.append(" ");

        sb.append(this.getGetTotalTps(600));

        return sb.toString();
    }

    private String getGetTransferedTps() {
        StringBuilder sb = new StringBuilder();

        sb.append(this.getGetTransferedTps(10));
        sb.append(" ");

        sb.append(this.getGetTransferedTps(60));
        sb.append(" ");

        sb.append(this.getGetTransferedTps(600));

        return sb.toString();
    }

    private String putMessageDistributeTimeToString() {
        final LongAdder[] times = this.lastPutMessageDistributeTime;
        if (null == times)
            return null;

        final StringBuilder sb = new StringBuilder();
        for (int i = 0; i < times.length; i++) {
            long value = times[i].longValue();
            sb.append(String.format("%s:%d", PUT_MESSAGE_ENTIRE_TIME_MAX_DESC[i], value));
            sb.append(" ");
        }

        return sb.toString();
    }

    private String getPutTps(int time) {
        String result = "";
        this.samplingLock.lock();
        try {
            CallSnapshot last = this.putTimesList.getLast();

            if (this.putTimesList.size() > time) {
                CallSnapshot lastBefore = this.putTimesList.get(this.putTimesList.size() - (time + 1));
                result += CallSnapshot.getTPS(lastBefore, last);
            }

        } finally {
            this.samplingLock.unlock();
        }
        return result;
    }

    private String getGetFoundTps(int time) {
        String result = "";
        this.samplingLock.lock();
        try {
            CallSnapshot last = this.getTimesFoundList.getLast();

            if (this.getTimesFoundList.size() > time) {
                CallSnapshot lastBefore =
                    this.getTimesFoundList.get(this.getTimesFoundList.size() - (time + 1));
                result += CallSnapshot.getTPS(lastBefore, last);
            }
        } finally {
            this.samplingLock.unlock();
        }

        return result;
    }

    private String getGetMissTps(int time) {
        String result = "";
        this.samplingLock.lock();
        try {
            CallSnapshot last = this.getTimesMissList.getLast();

            if (this.getTimesMissList.size() > time) {
                CallSnapshot lastBefore =
                    this.getTimesMissList.get(this.getTimesMissList.size() - (time + 1));
                result += CallSnapshot.getTPS(lastBefore, last);
            }

        } finally {
            this.samplingLock.unlock();
        }

        return result;
    }

    private String getGetTotalTps(int time) {
        this.samplingLock.lock();
        double found = 0;
        double miss = 0;
        try {
            {
                CallSnapshot last = this.getTimesFoundList.getLast();

                if (this.getTimesFoundList.size() > time) {
                    CallSnapshot lastBefore =
                        this.getTimesFoundList.get(this.getTimesFoundList.size() - (time + 1));
                    found = CallSnapshot.getTPS(lastBefore, last);
                }
            }
            {
                CallSnapshot last = this.getTimesMissList.getLast();

                if (this.getTimesMissList.size() > time) {
                    CallSnapshot lastBefore =
                        this.getTimesMissList.get(this.getTimesMissList.size() - (time + 1));
                    miss = CallSnapshot.getTPS(lastBefore, last);
                }
            }

        } finally {
            this.samplingLock.unlock();
        }

        return Double.toString(found + miss);
    }

    private String getGetTransferedTps(int time) {
        String result = "";
        this.samplingLock.lock();
        try {
            CallSnapshot last = this.transferedMsgCountList.getLast();

            if (this.transferedMsgCountList.size() > time) {
                CallSnapshot lastBefore =
                    this.transferedMsgCountList.get(this.transferedMsgCountList.size() - (time + 1));
                result += CallSnapshot.getTPS(lastBefore, last);
            }

        } finally {
            this.samplingLock.unlock();
        }

        return result;
    }

    public HashMap<String, String> getRuntimeInfo() {
        HashMap<String, String> result = new HashMap<String, String>(64);

        Long totalTimes = getPutMessageTimesTotal();
        if (0 == totalTimes) {
            totalTimes = 1L;
        }

        result.put("bootTimestamp", String.valueOf(this.messageStoreBootTimestamp));
        result.put("runtime", this.getFormatRuntime());
        result.put("putMessageEntireTimeMax", String.valueOf(this.putMessageEntireTimeMax));
        result.put("putMessageTimesTotal", String.valueOf(totalTimes));
        result.put("putMessageFailedTimes", String.valueOf(this.putMessageFailedTimes));
        result.put("putMessageSizeTotal", String.valueOf(this.getPutMessageSizeTotal()));
        result.put("putMessageDistributeTime",
            String.valueOf(this.getPutMessageDistributeTimeStringInfo(totalTimes)));
        result.put("putMessageAverageSize",
            String.valueOf(this.getPutMessageSizeTotal() / totalTimes.doubleValue()));
        result.put("dispatchMaxBuffer", String.valueOf(this.dispatchMaxBuffer));
        result.put("getMessageEntireTimeMax", String.valueOf(this.getMessageEntireTimeMax));
        result.put("putTps", this.getPutTps());
        result.put("getFoundTps", this.getGetFoundTps());
        result.put("getMissTps", this.getGetMissTps());
        result.put("getTotalTps", this.getGetTotalTps());
        result.put("getTransferedTps", this.getGetTransferedTps());
        result.put("putLatency99", String.format("%.2f", this.findPutMessageEntireTimePX(0.99)));
        result.put("putLatency999", String.format("%.2f", this.findPutMessageEntireTimePX(0.999)));

        return result;
    }

    public void run() {
        log.info(this.getServiceName() + " service started");

        while (!this.isStopped()) {
            try {
                this.waitForRunning(FREQUENCY_OF_SAMPLING);

                this.sampling();

                this.printTps();
            } catch (Exception e) {
                log.warn(this.getServiceName() + " service has exception. ", e);
            }
        }

        log.info(this.getServiceName() + " service end");
    }

    @Override
    public String getServiceName() {
        return StoreStatsService.class.getSimpleName();
    }

    private void sampling() {
        this.samplingLock.lock();
        try {
            this.putTimesList.add(new CallSnapshot(System.currentTimeMillis(), getPutMessageTimesTotal()));
            if (this.putTimesList.size() > (MAX_RECORDS_OF_SAMPLING + 1)) {
                this.putTimesList.removeFirst();
            }

            this.getTimesFoundList.add(new CallSnapshot(System.currentTimeMillis(),
                this.getMessageTimesTotalFound.longValue()));
            if (this.getTimesFoundList.size() > (MAX_RECORDS_OF_SAMPLING + 1)) {
                this.getTimesFoundList.removeFirst();
            }

            this.getTimesMissList.add(new CallSnapshot(System.currentTimeMillis(),
                this.getMessageTimesTotalMiss.longValue()));
            if (this.getTimesMissList.size() > (MAX_RECORDS_OF_SAMPLING + 1)) {
                this.getTimesMissList.removeFirst();
            }

            this.transferedMsgCountList.add(new CallSnapshot(System.currentTimeMillis(),
                this.getMessageTransferedMsgCount.longValue()));
            if (this.transferedMsgCountList.size() > (MAX_RECORDS_OF_SAMPLING + 1)) {
                this.transferedMsgCountList.removeFirst();
            }

        } finally {
            this.samplingLock.unlock();
        }
    }

    private void printTps() {
        if (System.currentTimeMillis() > (this.lastPrintTimestamp + printTPSInterval * 1000)) {
            this.lastPrintTimestamp = System.currentTimeMillis();

            log.info("[STORETPS] put_tps {} get_found_tps {} get_miss_tps {} get_transfered_tps {}",
                this.getPutTps(printTPSInterval),
                this.getGetFoundTps(printTPSInterval),
                this.getGetMissTps(printTPSInterval),
                this.getGetTransferedTps(printTPSInterval)
            );

            final LongAdder[] times = this.resetPutMessageDistributeTime();
            if (null == times)
                return;

            final StringBuilder sb = new StringBuilder();
            long totalPut = 0;
            for (int i = 0; i < times.length; i++) {
                long value = times[i].longValue();
                totalPut += value;
                sb.append(String.format("%s:%d", PUT_MESSAGE_ENTIRE_TIME_MAX_DESC[i], value));
                sb.append(" ");
            }
            this.resetPutMessageTimeBuckets();
            this.findPutMessageEntireTimePX(0.99);
            this.findPutMessageEntireTimePX(0.999);
            log.info("[PAGECACHERT] TotalPut {}, PutMessageDistributeTime {}", totalPut, sb.toString());
        }
    }

    public LongAdder getGetMessageTimesTotalFound() {
        return getMessageTimesTotalFound;
    }

    public LongAdder getGetMessageTimesTotalMiss() {
        return getMessageTimesTotalMiss;
    }

    public LongAdder getGetMessageTransferedMsgCount() {
        return getMessageTransferedMsgCount;
    }

    public LongAdder getPutMessageFailedTimes() {
        return putMessageFailedTimes;
    }

    public LongAdder getSinglePutMessageTopicSizeTotal(String topic) {
        LongAdder rs = putMessageTopicSizeTotal.get(topic);
        if (null == rs) {
            rs = new LongAdder();
            LongAdder previous = putMessageTopicSizeTotal.putIfAbsent(topic, rs);
            if (previous != null) {
                rs = previous;
            }
        }
        return rs;
    }

    public LongAdder getSinglePutMessageTopicTimesTotal(String topic) {
        LongAdder rs = putMessageTopicTimesTotal.get(topic);
        if (null == rs) {
            rs = new LongAdder();
            LongAdder previous = putMessageTopicTimesTotal.putIfAbsent(topic, rs);
            if (previous != null) {
                rs = previous;
            }
        }
        return rs;
    }

    public Map<String, LongAdder> getPutMessageTopicTimesTotal() {
        return putMessageTopicTimesTotal;
    }

    public Map<String, LongAdder> getPutMessageTopicSizeTotal() {
        return putMessageTopicSizeTotal;
    }

    static class CallSnapshot {
        public final long timestamp;
        public final long callTimesTotal;

        public CallSnapshot(long timestamp, long callTimesTotal) {
            this.timestamp = timestamp;
            this.callTimesTotal = callTimesTotal;
        }

        public static double getTPS(final CallSnapshot begin, final CallSnapshot end) {
            long total = end.callTimesTotal - begin.callTimesTotal;
            Long time = end.timestamp - begin.timestamp;

            double tps = total / time.doubleValue();

            return tps * 1000;
        }
    }
}
