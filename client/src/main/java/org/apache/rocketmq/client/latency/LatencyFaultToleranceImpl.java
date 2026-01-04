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

package org.apache.rocketmq.client.latency;

import java.util.Collections;
import java.util.Enumeration;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.client.common.ThreadLocalIndex;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

public class LatencyFaultToleranceImpl implements LatencyFaultTolerance<String> {
    private final static Logger log = LoggerFactory.getLogger(MQFaultStrategy.class);
    private final ConcurrentHashMap<String, FaultItem> faultItemTable = new ConcurrentHashMap<String, FaultItem>(16);
    private int detectTimeout = 200;
    private int detectInterval = 2000;
    private final ThreadLocalIndex whichItemWorst = new ThreadLocalIndex();

    private volatile boolean startDetectorEnable = false;
    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
            return new Thread(r, "LatencyFaultToleranceScheduledThread");
        }
    });

    private final Resolver resolver;

    private final ServiceDetector serviceDetector;

    public LatencyFaultToleranceImpl(Resolver resolver, ServiceDetector serviceDetector) {
        this.resolver = resolver;
        this.serviceDetector = serviceDetector;
    }

    @Override
    public void detectByOneRound() {
        for (Map.Entry<String, FaultItem> item : this.faultItemTable.entrySet()) {
            FaultItem brokerItem = item.getValue();
            if (System.currentTimeMillis() - brokerItem.checkStamp >= 0) {
                brokerItem.checkStamp = System.currentTimeMillis() + this.detectInterval;
                String brokerAddr = resolver.resolve(brokerItem.getName());
                if (brokerAddr == null) {
                    faultItemTable.remove(item.getKey());
                    continue;
                }
                if (null == serviceDetector) {
                    continue;
                }
                boolean serviceOK = serviceDetector.detect(brokerAddr, detectTimeout);
                if (serviceOK && !brokerItem.reachableFlag) {
                    log.info(brokerItem.name + " is reachable now, then it can be used.");
                    brokerItem.reachableFlag = true;
                }
            }
        }
    }

    @Override
    public void startDetector() {
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    if (startDetectorEnable) {
                        detectByOneRound();
                    }
                } catch (Exception e) {
                    log.warn("Unexpected exception raised while detecting service reachability", e);
                }
            }
        }, 3, 3, TimeUnit.SECONDS);
    }

    @Override
    public void shutdown() {
        this.scheduledExecutorService.shutdown();
    }

    @Override
    public void updateFaultItem(final String name, final long currentLatency, final long notAvailableDuration,
                                final boolean reachable) {
        FaultItem old = this.faultItemTable.get(name);
        if (null == old) {
            final FaultItem faultItem = new FaultItem(name);
            faultItem.setCurrentLatency(currentLatency);
            faultItem.updateNotAvailableDuration(notAvailableDuration);
            faultItem.setReachable(reachable);
            old = this.faultItemTable.putIfAbsent(name, faultItem);
        }

        if (null != old) {
            old.setCurrentLatency(currentLatency);
            old.updateNotAvailableDuration(notAvailableDuration);
            old.setReachable(reachable);
        }

        if (!reachable) {
            log.info(name + " is unreachable, it will not be used until it's reachable");
        }
    }

    @Override
    public boolean isAvailable(final String name) {
        final FaultItem faultItem = this.faultItemTable.get(name);
        if (faultItem != null) {
            return faultItem.isAvailable();
        }
        return true;
    }

    @Override
    public boolean isReachable(final String name) {
        final FaultItem faultItem = this.faultItemTable.get(name);
        if (faultItem != null) {
            return faultItem.isReachable();
        }
        return true;
    }

    @Override
    public void remove(final String name) {
        this.faultItemTable.remove(name);
    }

    @Override
    public boolean isStartDetectorEnable() {
        return startDetectorEnable;
    }

    @Override
    public void setStartDetectorEnable(boolean startDetectorEnable) {
        this.startDetectorEnable = startDetectorEnable;
    }
    @Override
    public String pickOneAtLeast() {
        final Enumeration<FaultItem> elements = this.faultItemTable.elements();
        List<FaultItem> tmpList = new LinkedList<FaultItem>();
        while (elements.hasMoreElements()) {
            final FaultItem faultItem = elements.nextElement();
            tmpList.add(faultItem);
        }

        if (!tmpList.isEmpty()) {
            Collections.shuffle(tmpList);
            for (FaultItem faultItem : tmpList) {
                if (faultItem.reachableFlag) {
                    return faultItem.name;
                }
            }
        }

        return null;
    }

    @Override
    public String toString() {
        return "LatencyFaultToleranceImpl{" +
                "faultItemTable=" + faultItemTable +
                ", whichItemWorst=" + whichItemWorst +
                '}';
    }

    @Override
    public void setDetectTimeout(final int detectTimeout) {
        this.detectTimeout = detectTimeout;
    }

    @Override
    public void setDetectInterval(final int detectInterval) {
        this.detectInterval = detectInterval;
    }

    public class FaultItem implements Comparable<FaultItem> {
        private final String name;
        private volatile long currentLatency;
        private volatile long startTimestamp;
        private volatile long checkStamp;
        private volatile boolean reachableFlag;

        public FaultItem(final String name) {
            this.name = name;
        }

        public void updateNotAvailableDuration(long notAvailableDuration) {
            if (notAvailableDuration > 0 && System.currentTimeMillis() + notAvailableDuration > this.startTimestamp) {
                this.startTimestamp = System.currentTimeMillis() + notAvailableDuration;
                log.info(name + " will be isolated for " + notAvailableDuration + " ms.");
            }
        }

        @Override
        public int compareTo(final FaultItem other) {
            if (this.isAvailable() != other.isAvailable()) {
                if (this.isAvailable()) {
                    return -1;
                }

                if (other.isAvailable()) {
                    return 1;
                }
            }

            if (this.currentLatency < other.currentLatency) {
                return -1;
            } else if (this.currentLatency > other.currentLatency) {
                return 1;
            }

            if (this.startTimestamp < other.startTimestamp) {
                return -1;
            } else if (this.startTimestamp > other.startTimestamp) {
                return 1;
            }
            return 0;
        }

        public void setReachable(boolean reachableFlag) {
            this.reachableFlag = reachableFlag;
        }

        public void setCheckStamp(long checkStamp) {
            this.checkStamp = checkStamp;
        }

        public boolean isAvailable() {
            return System.currentTimeMillis() >= startTimestamp;
        }

        public boolean isReachable() {
            return reachableFlag;
        }

        @Override
        public int hashCode() {
            int result = getName() != null ? getName().hashCode() : 0;
            result = 31 * result + (int) (getCurrentLatency() ^ (getCurrentLatency() >>> 32));
            result = 31 * result + (int) (getStartTimestamp() ^ (getStartTimestamp() >>> 32));
            return result;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof FaultItem)) {
                return false;
            }

            final FaultItem faultItem = (FaultItem) o;

            if (getCurrentLatency() != faultItem.getCurrentLatency()) {
                return false;
            }
            if (getStartTimestamp() != faultItem.getStartTimestamp()) {
                return false;
            }
            return getName() != null ? getName().equals(faultItem.getName()) : faultItem.getName() == null;
        }

        @Override
        public String toString() {
            return "FaultItem{" +
                    "name='" + name + '\'' +
                    ", currentLatency=" + currentLatency +
                    ", startTimestamp=" + startTimestamp +
                    ", reachableFlag=" + reachableFlag +
                    '}';
        }

        public String getName() {
            return name;
        }

        public long getCurrentLatency() {
            return currentLatency;
        }

        public void setCurrentLatency(final long currentLatency) {
            this.currentLatency = currentLatency;
        }

        public long getStartTimestamp() {
            return startTimestamp;
        }

        public void setStartTimestamp(final long startTimestamp) {
            this.startTimestamp = startTimestamp;
        }

    }
}
