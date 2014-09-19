/**
 * Copyright (C) 2010-2013 Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.rocketmq.store.index;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.rocketmq.common.ServiceThread;
import com.alibaba.rocketmq.common.UtilAll;
import com.alibaba.rocketmq.common.constant.LoggerName;
import com.alibaba.rocketmq.common.message.MessageConst;
import com.alibaba.rocketmq.common.sysflag.MessageSysFlag;
import com.alibaba.rocketmq.store.DefaultMessageStore;
import com.alibaba.rocketmq.store.DispatchRequest;
import com.alibaba.rocketmq.store.config.StorePathConfigHelper;


/**
 * 消息索引服务
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-7-21
 */
public class IndexService extends ServiceThread {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.StoreLoggerName);
    private final DefaultMessageStore defaultMessageStore;
    // 索引配置
    private final int hashSlotNum;
    private final int indexNum;
    private final String storePath;
    // 索引文件集合
    private final ArrayList<IndexFile> indexFileList = new ArrayList<IndexFile>();
    // 读写锁（针对indexFileList）
    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    private LinkedBlockingQueue<Object[]> requestQueue = new LinkedBlockingQueue<Object[]>(300000);


    public IndexService(final DefaultMessageStore store) {
        this.defaultMessageStore = store;
        this.hashSlotNum = store.getMessageStoreConfig().getMaxHashSlotNum();
        this.indexNum = store.getMessageStoreConfig().getMaxIndexNum();
        this.storePath =
                StorePathConfigHelper.getStorePathIndex(store.getMessageStoreConfig().getStorePathRootDir());
    }


    public boolean load(final boolean lastExitOK) {
        File dir = new File(this.storePath);
        File[] files = dir.listFiles();
        if (files != null) {
            // ascending order
            Arrays.sort(files);
            for (File file : files) {
                try {
                    IndexFile f = new IndexFile(file.getPath(), this.hashSlotNum, this.indexNum, 0, 0);
                    f.load();

                    if (!lastExitOK) {
                        if (f.getEndTimestamp() > this.defaultMessageStore.getStoreCheckpoint()
                            .getIndexMsgTimestamp()) {
                            f.destroy(0);
                            continue;
                        }
                    }

                    log.info("load index file OK, " + f.getFileName());
                    this.indexFileList.add(f);
                }
                catch (IOException e) {
                    log.error("load file " + file + " error", e);
                    return false;
                }
            }
        }

        return true;
    }


    /**
     * 删除索引文件
     */
    public void deleteExpiredFile(long offset) {
        Object[] files = null;
        try {
            this.readWriteLock.readLock().lock();
            if (this.indexFileList.isEmpty()) {
                return;
            }

            long endPhyOffset = this.indexFileList.get(0).getEndPhyOffset();
            if (endPhyOffset < offset) {
                files = this.indexFileList.toArray();
            }
        }
        catch (Exception e) {
            log.error("destroy exception", e);
        }
        finally {
            this.readWriteLock.readLock().unlock();
        }

        if (files != null) {
            List<IndexFile> fileList = new ArrayList<IndexFile>();
            for (int i = 0; i < (files.length - 1); i++) {
                IndexFile f = (IndexFile) files[i];
                if (f.getEndPhyOffset() < offset) {
                    fileList.add(f);
                }
                else {
                    break;
                }
            }

            this.deleteExpiredFile(fileList);
        }
    }


    /**
     * 删除文件只能从头开始删
     */
    private void deleteExpiredFile(List<IndexFile> files) {
        if (!files.isEmpty()) {
            try {
                this.readWriteLock.writeLock().lock();
                for (IndexFile file : files) {
                    boolean destroyed = file.destroy(3000);
                    destroyed = destroyed && this.indexFileList.remove(file);
                    if (!destroyed) {
                        log.error("deleteExpiredFile remove failed.");
                        break;
                    }
                }
            }
            catch (Exception e) {
                log.error("deleteExpiredFile has exception.", e);
            }
            finally {
                this.readWriteLock.writeLock().unlock();
            }
        }
    }


    public void destroy() {
        try {
            this.readWriteLock.readLock().lock();
            for (IndexFile f : this.indexFileList) {
                f.destroy(1000 * 3);
            }
            this.indexFileList.clear();
        }
        catch (Exception e) {
            log.error("destroy exception", e);
        }
        finally {
            this.readWriteLock.readLock().unlock();
        }
    }


    public QueryOffsetResult queryOffset(String topic, String key, int maxNum, long begin, long end) {
        List<Long> phyOffsets = new ArrayList<Long>(maxNum);
        // TODO 可能需要返回给最终用户
        long indexLastUpdateTimestamp = 0;
        long indexLastUpdatePhyoffset = 0;
        maxNum = Math.min(maxNum, this.defaultMessageStore.getMessageStoreConfig().getMaxMsgsNumBatch());
        try {
            this.readWriteLock.readLock().lock();
            if (!this.indexFileList.isEmpty()) {
                for (int i = this.indexFileList.size(); i > 0; i--) {
                    IndexFile f = this.indexFileList.get(i - 1);
                    boolean lastFile = (i == this.indexFileList.size());
                    if (lastFile) {
                        indexLastUpdateTimestamp = f.getEndTimestamp();
                        indexLastUpdatePhyoffset = f.getEndPhyOffset();
                    }

                    if (f.isTimeMatched(begin, end)) {
                        // 最后一个文件需要加锁
                        f.selectPhyOffset(phyOffsets, this.buildKey(topic, key), maxNum, begin, end, lastFile);
                    }

                    // 再往前遍历时间更不符合
                    if (f.getBeginTimestamp() < begin) {
                        break;
                    }

                    if (phyOffsets.size() >= maxNum) {
                        break;
                    }
                }
            }
        }
        catch (Exception e) {
            log.error("queryMsg exception", e);
        }
        finally {
            this.readWriteLock.readLock().unlock();
        }

        return new QueryOffsetResult(phyOffsets, indexLastUpdateTimestamp, indexLastUpdatePhyoffset);
    }


    private String buildKey(final String topic, final String key) {
        return topic + "#" + key;
    }


    /**
     * 向队列中添加请求，队列满情况下，丢弃请求
     */
    public void putRequest(final Object[] reqs) {
        boolean offer = this.requestQueue.offer(reqs);
        if (!offer) {
            if (log.isDebugEnabled()) {
                log.debug("putRequest index failed, {}", reqs);
            }
        }
    }


    @Override
    public void run() {
        log.info(this.getServiceName() + " service started");

        while (!this.isStoped()) {
            try {
                Object[] req = this.requestQueue.poll(3000, TimeUnit.MILLISECONDS);

                if (req != null) {
                    this.buildIndex(req);
                }
            }
            catch (Exception e) {
                log.warn(this.getServiceName() + " service has exception. ", e);
            }
        }

        log.info(this.getServiceName() + " service end");
    }


    public void buildIndex(Object[] req) {
        boolean breakdown = false;
        IndexFile indexFile = retryGetAndCreateIndexFile();
        if (indexFile != null) {
            long endPhyOffset = indexFile.getEndPhyOffset();
            MSG_WHILE: for (Object o : req) {
                DispatchRequest msg = (DispatchRequest) o;
                String topic = msg.getTopic();
                String keys = msg.getKeys();
                if (msg.getCommitLogOffset() < endPhyOffset) {
                    continue;
                }

                final int tranType = MessageSysFlag.getTransactionValue(msg.getSysFlag());
                switch (tranType) {
                case MessageSysFlag.TransactionNotType:
                case MessageSysFlag.TransactionPreparedType:
                    break;
                case MessageSysFlag.TransactionCommitType:
                case MessageSysFlag.TransactionRollbackType:
                    continue;
                }

                if (keys != null && keys.length() > 0) {
                    String[] keyset = keys.split(MessageConst.KEY_SEPARATOR);
                    for (String key : keyset) {
                        // TODO 是否需要TRIM
                        if (key.length() > 0) {
                            for (boolean ok =
                                    indexFile.putKey(buildKey(topic, key), msg.getCommitLogOffset(),
                                        msg.getStoreTimestamp()); !ok;) {
                                log.warn("index file full, so create another one, " + indexFile.getFileName());
                                indexFile = retryGetAndCreateIndexFile();
                                if (null == indexFile) {
                                    breakdown = true;
                                    break MSG_WHILE;
                                }

                                ok =
                                        indexFile.putKey(buildKey(topic, key), msg.getCommitLogOffset(),
                                            msg.getStoreTimestamp());
                            }
                        }
                    }
                }
            }
        }
        // IO发生故障，build索引过程中断，需要人工参与处理
        else {
            breakdown = true;
        }

        if (breakdown) {
            log.error("build index error, stop building index");
        }
    }


    public IndexFile retryGetAndCreateIndexFile() {
        IndexFile indexFile = null;

        // 如果创建失败，尝试重建3次
        for (int times = 0; null == indexFile && times < 3; times++) {
            indexFile = this.getAndCreateLastIndexFile();
            if (null != indexFile)
                break;

            try {
                log.error("try to create index file, " + times + " times");
                Thread.sleep(1000);
            }
            catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        // 重试多次，仍然无法创建索引文件
        if (null == indexFile) {
            this.defaultMessageStore.getAccessRights().makeIndexFileError();
            log.error("mark index file can not build flag");
        }

        return indexFile;
    }


    /**
     * 获取最后一个索引文件，如果集合为空或者最后一个文件写满了，则新建一个文件<br>
     * 只有一个线程调用，所以不存在写竟争问题
     */
    public IndexFile getAndCreateLastIndexFile() {
        IndexFile indexFile = null;
        IndexFile prevIndexFile = null;
        long lastUpdateEndPhyOffset = 0;
        long lastUpdateIndexTimestamp = 0;
        // 先尝试使用读锁
        {
            this.readWriteLock.readLock().lock();
            if (!this.indexFileList.isEmpty()) {
                IndexFile tmp = this.indexFileList.get(this.indexFileList.size() - 1);
                if (!tmp.isWriteFull()) {
                    indexFile = tmp;
                }
                else {
                    lastUpdateEndPhyOffset = tmp.getEndPhyOffset();
                    lastUpdateIndexTimestamp = tmp.getEndTimestamp();
                    prevIndexFile = tmp;
                }
            }

            this.readWriteLock.readLock().unlock();
        }

        // 如果没找到，使用写锁创建文件
        if (indexFile == null) {
            try {
                String fileName =
                        this.storePath + File.separator
                                + UtilAll.timeMillisToHumanString(System.currentTimeMillis());
                indexFile =
                        new IndexFile(fileName, this.hashSlotNum, this.indexNum, lastUpdateEndPhyOffset,
                            lastUpdateIndexTimestamp);
                this.readWriteLock.writeLock().lock();
                this.indexFileList.add(indexFile);
            }
            catch (Exception e) {
                log.error("getLastIndexFile exception ", e);
            }
            finally {
                this.readWriteLock.writeLock().unlock();
            }

            // 每创建一个新文件，之前文件要刷盘
            if (indexFile != null) {
                final IndexFile flushThisFile = prevIndexFile;
                Thread flushThread = new Thread(new Runnable() {
                    @Override
                    public void run() {
                        IndexService.this.flush(flushThisFile);
                    }
                }, "FlushIndexFileThread");

                flushThread.setDaemon(true);
                flushThread.start();
            }
        }

        return indexFile;
    }


    public void flush(final IndexFile f) {
        if (null == f)
            return;

        long indexMsgTimestamp = 0;

        if (f.isWriteFull()) {
            indexMsgTimestamp = f.getEndTimestamp();
        }

        f.flush();

        if (indexMsgTimestamp > 0) {
            this.defaultMessageStore.getStoreCheckpoint().setIndexMsgTimestamp(indexMsgTimestamp);
            this.defaultMessageStore.getStoreCheckpoint().flush();
        }
    }


    @Override
    public String getServiceName() {
        return IndexService.class.getSimpleName();
    }
}
