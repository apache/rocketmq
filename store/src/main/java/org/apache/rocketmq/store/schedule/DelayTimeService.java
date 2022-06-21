
package org.apache.rocketmq.store.schedule;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.*;

/**
 * delay time service
 * 1.load file
 * 2.try get or create file
 */
public class DelayTimeService {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    private final DefaultMessageStore defaultMessageStore;
    private final String storePath;
    private List<DelayTimeLineFile> fileList = new ArrayList<>();
    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    private final DelayLinkLog delayLinkLog;
    private final Timer timer ;

    public DelayTimeService(final DefaultMessageStore defaultMessageStore) {
        String storePath = defaultMessageStore.getMessageStoreConfig().getStorePathDelayTimeLine();
        this.storePath = storePath;
        this.delayLinkLog = new DelayLinkLog(defaultMessageStore);
        this.defaultMessageStore = defaultMessageStore;
        timer = new Timer();
    }

    public void checkSelf() {
        delayLinkLog.checkSelf();
    }

    public long getFirstOffset() {
        if (fileList.isEmpty()) {
            return 0L;
        }
        final DelayTimeLineFile delayTimeLineFile = fileList.get(0);
        return delayTimeLineFile.getFirstOffset() * 1000;
    }

    public boolean load(final boolean lastExitOK) {
        File dir = new File(this.storePath);
        File[] files = dir.listFiles();
        if (files != null) {
            // ascending order
            Arrays.sort(files);
            for (File file : files) {
                try {
                    DelayTimeLineFile f = new DelayTimeLineFile(file.getPath(),
                        defaultMessageStore.getMessageStoreConfig().getDelayTimeLineSeconds(),
                        UtilAll.humanStringToTimeMillis(file.getName() + "000") / 1000, delayLinkLog);
                    f.load();

                    if (!lastExitOK) {
                        // 未正常退出
                        if (f.getLastStoreTime() > this.defaultMessageStore.getStoreCheckpoint()
                            .getDelayTimeTimestamp()) {
                            f.truncateDirtyDelayFile(this.defaultMessageStore.getStoreCheckpoint()
                                .getDelayTimeTimestamp());
                            continue;
                        }
                    }

                    log.info("load delay time line file OK, " + f.getFileName());
                    this.fileList.add(f);
                } catch (IOException e) {
                    log.error("load file {} error", file, e);
                    return false;
                } catch (NumberFormatException e) {
                    log.error("load file {} error", file, e);
                }
            }
        }
        delayLinkLog.load();
        return true;
    }

    public DelayTimeResult queryByTimestamp(long timestamp) {
        final DelayTimeLineFile delayTimeLineFile = getAndCreateLastDelayTimeFile(timestamp, false);
        if (delayTimeLineFile == null) {
            return null;
        }
        return delayTimeLineFile.queryByTimestamp(timestamp / 1000);
    }

    public void buildDelayTime(DispatchRequest req) {
        DelayTimeLineFile delayTimeLineFile = retryGetAndCreateDelayTimeFile(req.getTagsCode());
        if (delayTimeLineFile != null) {
            delayTimeLineFile.putTimeTask(req.getCommitLogOffset(), req.getTagsCode() / 1000, req.getStoreTimestamp()
                , req.getMsgSize());
        } else {
            log.error("build delay time error, stop building time");
        }
    }

    // 获取或创建新的DelayTimeFile
    public DelayTimeLineFile retryGetAndCreateDelayTimeFile(Long expireTime) {
        DelayTimeLineFile delayTimeLineFile = null;

        for (int times = 0; null == delayTimeLineFile && times < 3; times++) {
            delayTimeLineFile = this.getAndCreateLastDelayTimeFile(expireTime, true);
            if (null != delayTimeLineFile) {
                break;
            }

            try {
                log.info("Tried to create DelayTimeLine file " + times + " times");
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                log.error("Interrupted", e);
            }
        }

        if (null == delayTimeLineFile) {
//            this.defaultMessageStore.getAccessRights().makeIndexFileError();
            log.error("Mark DelayTimeLine file cannot build flag");
        }

        return delayTimeLineFile;
    }

    public DelayTimeLineFile getAndCreateLastDelayTimeFile(Long expireTime, boolean create) {
        DelayTimeLineFile delayTimeLineFile = null;
        DelayTimeLineFile prevDelayTimeLineFile = null;

        {
            this.readWriteLock.readLock().lock();
            int size = this.fileList.size();
            if (!this.fileList.isEmpty()) {
                for (int i = 0; i < size; i++) {
                    DelayTimeLineFile tmp = this.fileList.get(i);
                    if (tmp.isIncludeInTime(expireTime / 1000)) {
                        delayTimeLineFile = tmp;
                        break;
                    }
                    prevDelayTimeLineFile = tmp;
                }
            }

            this.readWriteLock.readLock().unlock();
        }

        if (delayTimeLineFile == null && create) {
            try {
                long createTime = System.currentTimeMillis();
                if (createTime > expireTime) {
                    createTime = prevDelayTimeLineFile.getFirstOffset() * 1000
                        + defaultMessageStore.getMessageStoreConfig().getDelayTimeLineSeconds() * 1000;
                }
                String fileName =
                    this.storePath + File.separator
                        + UtilAll.timeMillisToHumanString(createTime).substring(0, 14);
                delayTimeLineFile =
                    new DelayTimeLineFile(fileName,
                        defaultMessageStore.getMessageStoreConfig().getDelayTimeLineSeconds(),
                        createTime / 1000, delayLinkLog);
                this.readWriteLock.writeLock().lock();
                this.fileList.add(delayTimeLineFile);
            } catch (Exception e) {
                log.error("getLastDelayTimeLineFile exception ", e);
            } finally {
                this.readWriteLock.writeLock().unlock();
            }

        }

        return delayTimeLineFile;
    }


    public void start() {
        timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                flush();
            }
        }, 5000, 5000);
    }

    public void flush() {
        if (fileList.isEmpty()) {
            return;
        }
        long delayTimeLineTimestamp = 0L;
        for (DelayTimeLineFile file : fileList) {
            final long lastStoreTime = file.getLastStoreTime();
            file.flush();
            delayTimeLineTimestamp = Math.max(delayTimeLineTimestamp, lastStoreTime);
        }
        if (delayTimeLineTimestamp > 0L) {
            this.defaultMessageStore.getStoreCheckpoint().setDelayTimeTimestamp(delayTimeLineTimestamp);
        }
    }

    public void shutdown() {
        flush();
        if (this.timer != null) {
            this.timer.cancel();
        }
        delayLinkLog.shutdown();
    }

    public void tryDeleteExpireFiles() {
        if (!fileList.isEmpty()) {
            final DelayTimeLineFile delayTimeLineFile = fileList.get(0);
            final long lastStoreTime = delayTimeLineFile.getLastStoreTime();
            if (System.currentTimeMillis() - lastStoreTime
                > defaultMessageStore.getMessageStoreConfig().getDelayTimeLineSeconds() * 1000) {
                delayTimeLineFile.destroy(1000 * 3);
                fileList.remove(0);
            }
        }
        delayLinkLog
            .deleteExpiredFile(defaultMessageStore.getMessageStoreConfig().getDelayTimeLineSeconds() * 1000, 100,
                1000 * 120, true);
    }
}