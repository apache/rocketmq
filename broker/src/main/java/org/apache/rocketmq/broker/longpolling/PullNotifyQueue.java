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
package org.apache.rocketmq.broker.longpolling;

import java.util.LinkedList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

class PullNotifyQueue<T> {
    private PullNotifyQueueConfig config;

    private final LinkedBlockingQueue<T> queue;

    private int activeConsumeQueueCount = 1;
    private long activeConsumeQueueEpochTime = System.nanoTime();
    // only change in unit test
    private long activeConsumeQueueRefreshTime = 1_000_000_000;

    private long lastBatchDrainTime;

    public PullNotifyQueue(PullNotifyQueueConfig config) {
        queue = new LinkedBlockingQueue<>(500_000);
        this.config = config;
    }

    public void put(T data) throws InterruptedException {
        queue.put(data);
    }

    public LinkedList<T> drain() throws InterruptedException {
        LinkedList<T> result = new LinkedList<>();
        long tps = config.getTps();
        if (tps <= config.getTpsThreshold()) {
            T data = queue.poll(100, TimeUnit.MILLISECONDS);
            if (data != null) {
                result.add(data);
            }
        } else {
            long startTime = System.nanoTime();
            while (true) {
                queue.drainTo(result, config.getMaxBatchSize());
                if (result.size() >= config.getMaxBatchSize()) {
                    break;
                }
                if (result.size() / this.activeConsumeQueueCount >= config.getSuggestAvgBatchEachQueue()) {
                    break;
                }
                long restTime = config.getMaxLatencyNs() - (System.nanoTime() - startTime);
                if (restTime <= 0) {
                    break;
                }
                Thread.sleep(1);
            }
            this.lastBatchDrainTime = System.nanoTime() - startTime;
        }

        return result;
    }

    public int getActiveConsumeQueueCount() {
        return activeConsumeQueueCount;
    }

    public long getLastBatchDrainTime() {
        return lastBatchDrainTime;
    }

    public void setActiveConsumeQueueRefreshTime(long activeConsumeQueueRefreshTime) {
        this.activeConsumeQueueRefreshTime = activeConsumeQueueRefreshTime;
    }

    public void updateActiveConsumeQueueCount(int newCount) {
        long now = System.nanoTime();
        int oldCount = this.activeConsumeQueueCount;
        if (newCount > oldCount) {
            this.activeConsumeQueueCount = newCount;
            this.activeConsumeQueueEpochTime = now;
        } else {
            if (now - this.activeConsumeQueueEpochTime > this.activeConsumeQueueRefreshTime) {
                this.activeConsumeQueueCount = Math.max(Math.max(newCount, oldCount / 2), 1);
                this.activeConsumeQueueEpochTime = now;
            }
        }
    }

    static class PullNotifyQueueConfig {
        private int tpsThreshold;
        private int maxBatchSize;
        private long maxLatencyNs;
        private int suggestAvgBatchEachQueue;
        private int tps;

        public int getMaxBatchSize() {
            return maxBatchSize;
        }

        public void setMaxBatchSize(int maxBatchSize) {
            this.maxBatchSize = maxBatchSize;
        }

        public int getTpsThreshold() {
            return tpsThreshold;
        }

        public void setTpsThreshold(int tpsThreshold) {
            this.tpsThreshold = tpsThreshold;
        }

        public long getMaxLatencyNs() {
            return maxLatencyNs;
        }

        public void setMaxLatencyNs(long maxLatencyNs) {
            this.maxLatencyNs = maxLatencyNs;
        }

        public int getSuggestAvgBatchEachQueue() {
            return suggestAvgBatchEachQueue;
        }

        public void setSuggestAvgBatchEachQueue(int suggestAvgBatchEachQueue) {
            this.suggestAvgBatchEachQueue = suggestAvgBatchEachQueue;
        }

        public int getTps() {
            return tps;
        }

        public void setTps(int tps) {
            this.tps = tps;
        }
    }
}