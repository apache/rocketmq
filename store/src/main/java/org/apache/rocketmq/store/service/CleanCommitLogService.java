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
package org.apache.rocketmq.store.service;

import java.util.HashSet;
import java.util.Set;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.config.StorePathConfigHelper;
import org.apache.rocketmq.store.ha.autoswitch.AutoSwitchHAService;

public class CleanCommitLogService {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    private final static int MAX_MANUAL_DELETE_FILE_TIMES = 20;
    private final String diskSpaceWarningLevelRatio =
        System.getProperty("rocketmq.broker.diskSpaceWarningLevelRatio", "");

    private final String diskSpaceCleanForciblyRatio =
        System.getProperty("rocketmq.broker.diskSpaceCleanForciblyRatio", "");
    private long lastRedeleteTimestamp = 0;

    private volatile int manualDeleteFileSeveralTimes = 0;

    private volatile boolean cleanImmediately = false;

    private int forceCleanFailedTimes = 0;

    private final DefaultMessageStore messageStore;

    public CleanCommitLogService(DefaultMessageStore messageStore) {
        this.messageStore = messageStore;
    }


    public double getDiskSpaceWarningLevelRatio() {
        double finalDiskSpaceWarningLevelRatio;
        if ("".equals(diskSpaceWarningLevelRatio)) {
            finalDiskSpaceWarningLevelRatio = messageStore.getMessageStoreConfig().getDiskSpaceWarningLevelRatio() / 100.0;
        } else {
            finalDiskSpaceWarningLevelRatio = Double.parseDouble(diskSpaceWarningLevelRatio);
        }

        if (finalDiskSpaceWarningLevelRatio > 0.90) {
            finalDiskSpaceWarningLevelRatio = 0.90;
        }
        if (finalDiskSpaceWarningLevelRatio < 0.35) {
            finalDiskSpaceWarningLevelRatio = 0.35;
        }

        return finalDiskSpaceWarningLevelRatio;
    }

    public double getDiskSpaceCleanForciblyRatio() {
        double finalDiskSpaceCleanForciblyRatio;
        if ("".equals(diskSpaceCleanForciblyRatio)) {
            finalDiskSpaceCleanForciblyRatio = messageStore.getMessageStoreConfig().getDiskSpaceCleanForciblyRatio() / 100.0;
        } else {
            finalDiskSpaceCleanForciblyRatio = Double.parseDouble(diskSpaceCleanForciblyRatio);
        }

        if (finalDiskSpaceCleanForciblyRatio > 0.85) {
            finalDiskSpaceCleanForciblyRatio = 0.85;
        }
        if (finalDiskSpaceCleanForciblyRatio < 0.30) {
            finalDiskSpaceCleanForciblyRatio = 0.30;
        }

        return finalDiskSpaceCleanForciblyRatio;
    }

    public void executeDeleteFilesManually() {
        this.manualDeleteFileSeveralTimes = MAX_MANUAL_DELETE_FILE_TIMES;
        LOGGER.info("executeDeleteFilesManually was invoked");
    }

    public void run() {
        try {
            this.deleteExpiredFiles();
            this.reDeleteHangedFile();
        } catch (Throwable e) {
            LOGGER.warn(this.getServiceName() + " service has exception. ", e);
        }
    }

    private void deleteExpiredFiles() {
        int deleteCount = 0;
        long fileReservedTime = messageStore.getMessageStoreConfig().getFileReservedTime();
        int deletePhysicFilesInterval = messageStore.getMessageStoreConfig().getDeleteCommitLogFilesInterval();
        int destroyMappedFileIntervalForcibly = messageStore.getMessageStoreConfig().getDestroyMapedFileIntervalForcibly();
        int deleteFileBatchMax = messageStore.getMessageStoreConfig().getDeleteFileBatchMax();

        boolean isTimeUp = this.isTimeToDelete();
        boolean isUsageExceedsThreshold = this.isSpaceToDelete();
        boolean isManualDelete = this.manualDeleteFileSeveralTimes > 0;

        if (isTimeUp || isUsageExceedsThreshold || isManualDelete) {

            if (isManualDelete) {
                this.manualDeleteFileSeveralTimes--;
            }

            boolean cleanAtOnce = messageStore.getMessageStoreConfig().isCleanFileForciblyEnable() && this.cleanImmediately;

            LOGGER.info("begin to delete before {} hours file. isTimeUp: {} isUsageExceedsThreshold: {} manualDeleteFileSeveralTimes: {} cleanAtOnce: {} deleteFileBatchMax: {}",
                fileReservedTime,
                isTimeUp,
                isUsageExceedsThreshold,
                manualDeleteFileSeveralTimes,
                cleanAtOnce,
                deleteFileBatchMax);

            fileReservedTime *= 60 * 60 * 1000;

            deleteCount = messageStore.getCommitLog().deleteExpiredFile(fileReservedTime, deletePhysicFilesInterval,
                destroyMappedFileIntervalForcibly, cleanAtOnce, deleteFileBatchMax);
            if (deleteCount > 0) {
                // If in the controller mode, we should notify the AutoSwitchHaService to truncateEpochFile
                if (messageStore.getBrokerConfig().isEnableControllerMode()) {
                    if (messageStore.getHaService() instanceof AutoSwitchHAService) {
                        final long minPhyOffset = messageStore.getMinPhyOffset();
                        ((AutoSwitchHAService) messageStore.getHaService()).truncateEpochFilePrefix(minPhyOffset - 1);
                    }
                }
            } else if (isUsageExceedsThreshold) {
                LOGGER.warn("disk space will be full soon, but delete file failed.");
            }
        }
    }

    private void reDeleteHangedFile() {
        int interval = messageStore.getMessageStoreConfig().getRedeleteHangedFileInterval();
        long currentTimestamp = System.currentTimeMillis();
        if ((currentTimestamp - this.lastRedeleteTimestamp) > interval) {
            this.lastRedeleteTimestamp = currentTimestamp;
            int destroyMappedFileIntervalForcibly =
                messageStore.getMessageStoreConfig().getDestroyMapedFileIntervalForcibly();
            if (messageStore.getCommitLog().retryDeleteFirstFile(destroyMappedFileIntervalForcibly)) {
            }
        }
    }

    public String getServiceName() {
        return messageStore.getBrokerConfig().getIdentifier() + CleanCommitLogService.class.getSimpleName();
    }

    private boolean isTimeToDelete() {
        String when = messageStore.getMessageStoreConfig().getDeleteWhen();
        if (UtilAll.isItTimeToDo(when)) {
            LOGGER.info("it's time to reclaim disk space, " + when);
            return true;
        }

        return false;
    }

    private boolean isSpaceToDelete() {
        cleanImmediately = false;

        String commitLogStorePath = messageStore.getMessageStoreConfig().getStorePathCommitLog();
        String[] storePaths = commitLogStorePath.trim().split(MixAll.MULTI_PATH_SPLITTER);
        Set<String> fullStorePath = new HashSet<>();
        double minPhysicRatio = 100;
        String minStorePath = null;
        for (String storePathPhysic : storePaths) {
            double physicRatio = UtilAll.getDiskPartitionSpaceUsedPercent(storePathPhysic);
            if (minPhysicRatio > physicRatio) {
                minPhysicRatio = physicRatio;
                minStorePath = storePathPhysic;
            }
            if (physicRatio > getDiskSpaceCleanForciblyRatio()) {
                fullStorePath.add(storePathPhysic);
            }
        }
        messageStore.getCommitLog().setFullStorePaths(fullStorePath);
        if (minPhysicRatio > getDiskSpaceWarningLevelRatio()) {
            boolean diskFull = messageStore.getRunningFlags().getAndMakeDiskFull();
            if (diskFull) {
                LOGGER.error("physic disk maybe full soon " + minPhysicRatio +
                    ", so mark disk full, storePathPhysic=" + minStorePath);
            }

            cleanImmediately = true;
            return true;
        } else if (minPhysicRatio > getDiskSpaceCleanForciblyRatio()) {
            cleanImmediately = true;
            return true;
        } else {
            boolean diskOK = messageStore.getRunningFlags().getAndMakeDiskOK();
            if (!diskOK) {
                LOGGER.info("physic disk space OK " + minPhysicRatio +
                    ", so mark disk ok, storePathPhysic=" + minStorePath);
            }
        }

        String storePathLogics = StorePathConfigHelper
            .getStorePathConsumeQueue(messageStore.getMessageStoreConfig().getStorePathRootDir());
        double logicsRatio = UtilAll.getDiskPartitionSpaceUsedPercent(storePathLogics);
        if (logicsRatio > getDiskSpaceWarningLevelRatio()) {
            boolean diskOK = messageStore.getRunningFlags().getAndMakeDiskFull();
            if (diskOK) {
                LOGGER.error("logics disk maybe full soon " + logicsRatio + ", so mark disk full");
            }

            cleanImmediately = true;
            return true;
        } else if (logicsRatio > getDiskSpaceCleanForciblyRatio()) {
            cleanImmediately = true;
            return true;
        } else {
            boolean diskOK = messageStore.getRunningFlags().getAndMakeDiskOK();
            if (!diskOK) {
                LOGGER.info("logics disk space OK " + logicsRatio + ", so mark disk ok");
            }
        }

        double ratio = messageStore.getMessageStoreConfig().getDiskMaxUsedSpaceRatio() / 100.0;
        int replicasPerPartition = messageStore.getMessageStoreConfig().getReplicasPerDiskPartition();
        // Only one commitLog in node
        if (replicasPerPartition <= 1) {
            if (minPhysicRatio < 0 || minPhysicRatio > ratio) {
                LOGGER.info("commitLog disk maybe full soon, so reclaim space, " + minPhysicRatio);
                return true;
            }

            if (logicsRatio < 0 || logicsRatio > ratio) {
                LOGGER.info("consumeQueue disk maybe full soon, so reclaim space, " + logicsRatio);
                return true;
            }
            return false;
        } else {
            long majorFileSize = messageStore.getMajorFileSize();
            long partitionLogicalSize = UtilAll.getDiskPartitionTotalSpace(minStorePath) / replicasPerPartition;
            double logicalRatio = 1.0 * majorFileSize / partitionLogicalSize;

            if (logicalRatio > messageStore.getMessageStoreConfig().getLogicalDiskSpaceCleanForciblyThreshold()) {
                // if logical ratio exceeds 0.80, then clean immediately
                LOGGER.info("Logical disk usage {} exceeds logical disk space clean forcibly threshold {}, forcibly: {}",
                    logicalRatio, minPhysicRatio, cleanImmediately);
                cleanImmediately = true;
                return true;
            }

            boolean isUsageExceedsThreshold = logicalRatio > ratio;
            if (isUsageExceedsThreshold) {
                LOGGER.info("Logical disk usage {} exceeds clean threshold {}, forcibly: {}",
                    logicalRatio, ratio, cleanImmediately);
            }
            return isUsageExceedsThreshold;
        }
    }

    public int getManualDeleteFileSeveralTimes() {
        return manualDeleteFileSeveralTimes;
    }

    public void setManualDeleteFileSeveralTimes(int manualDeleteFileSeveralTimes) {
        this.manualDeleteFileSeveralTimes = manualDeleteFileSeveralTimes;
    }

    public double calcStorePathPhysicRatio() {
        Set<String> fullStorePath = new HashSet<>();
        String storePath = messageStore.getStorePathPhysic();
        String[] paths = storePath.trim().split(MixAll.MULTI_PATH_SPLITTER);
        double minPhysicRatio = 100;
        for (String path : paths) {
            double physicRatio = UtilAll.isPathExists(path) ?
                UtilAll.getDiskPartitionSpaceUsedPercent(path) : -1;
            minPhysicRatio = Math.min(minPhysicRatio, physicRatio);
            if (physicRatio > getDiskSpaceCleanForciblyRatio()) {
                fullStorePath.add(path);
            }
        }
        messageStore.getCommitLog().setFullStorePaths(fullStorePath);
        return minPhysicRatio;

    }

    public boolean isSpaceFull() {
        double physicRatio = calcStorePathPhysicRatio();
        double ratio = messageStore.getMessageStoreConfig().getDiskMaxUsedSpaceRatio() / 100.0;
        if (physicRatio > ratio) {
            LOGGER.info("physic disk of commitLog used: " + physicRatio);
        }
        if (physicRatio > this.getDiskSpaceWarningLevelRatio()) {
            boolean diskok = messageStore.getRunningFlags().getAndMakeDiskFull();
            if (diskok) {
                LOGGER.error("physic disk of commitLog maybe full soon, used " + physicRatio + ", so mark disk full");
            }

            return true;
        } else {
            boolean diskok = messageStore.getRunningFlags().getAndMakeDiskOK();

            if (!diskok) {
                LOGGER.info("physic disk space of commitLog OK " + physicRatio + ", so mark disk ok");
            }

            return false;
        }
    }
}
