package com.alibaba.rocketmq.namesrv.kvconfig;

import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.rocketmq.common.MixAll;
import com.alibaba.rocketmq.common.constant.LoggerName;
import com.alibaba.rocketmq.namesrv.NamesrvController;
import com.alibaba.rocketmq.remoting.protocol.RemotingSerializable;


/**
 * KV≈‰÷√π‹¿Ì
 * 
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-7-1
 */
public class KVConfigManager {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.NamesrvLoggerName);

    private final NamesrvController namesrvController;

    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final HashMap<String/* Namespace */, HashMap<String/* Key */, String/* Value */>> configTable =
            new HashMap<String, HashMap<String, String>>();


    public KVConfigManager(NamesrvController namesrvController) {
        this.namesrvController = namesrvController;
    }


    public void load() {
        String content = MixAll.file2String(this.namesrvController.getNamesrvConfig().getKvConfigPath());
        if (content != null) {
            KVConfigSerializeWrapper kvConfigSerializeWrapper =
                    KVConfigSerializeWrapper.fromJson(content, KVConfigSerializeWrapper.class);
            if (null != kvConfigSerializeWrapper) {
                this.configTable.putAll(kvConfigSerializeWrapper.getConfigTable());
                log.info("load KV config table OK");
            }
        }
    }


    public void putKVConfig(final String namespace, final String key, final String value) {
        try {
            this.lock.writeLock().lockInterruptibly();
            try {
                HashMap<String, String> kvTable = this.configTable.get(namespace);
                if (null == kvTable) {
                    kvTable = new HashMap<String, String>();
                    this.configTable.put(namespace, kvTable);
                    log.info("putKVConfig create new Namespace {}", namespace);
                }

                final String prev = kvTable.put(key, value);
                if (null != prev) {
                    log.info("putKVConfig update config item, Namespace: {} Key: {} Value: {}", //
                        namespace, key, value);
                }
                else {
                    log.info("putKVConfig create new config item, Namespace: {} Key: {} Value: {}", //
                        namespace, key, value);
                }
            }
            finally {
                this.lock.writeLock().unlock();
            }
        }
        catch (InterruptedException e) {
            log.error("putKVConfig InterruptedException", e);
        }

        this.persist();
    }


    public void deleteKVConfig(final String namespace, final String key) {
        try {
            this.lock.writeLock().lockInterruptibly();
            try {
                HashMap<String, String> kvTable = this.configTable.get(namespace);
                if (null != kvTable) {
                    String value = kvTable.remove(key);
                    log.info("deleteKVConfig delete a config item, Namespace: {} Key: {} Value: {}", //
                        namespace, key, value);
                }
            }
            finally {
                this.lock.writeLock().unlock();
            }
        }
        catch (InterruptedException e) {
            log.error("deleteKVConfig InterruptedException", e);
        }

        this.persist();
    }


    public String getKVConfig(final String namespace, final String key) {
        try {
            this.lock.readLock().lockInterruptibly();
            try {
                HashMap<String, String> kvTable = this.configTable.get(namespace);
                if (null != kvTable) {
                    return kvTable.get(key);
                }
            }
            finally {
                this.lock.readLock().unlock();
            }
        }
        catch (InterruptedException e) {
            log.error("getKVConfig InterruptedException", e);
        }

        return null;
    }


    public void persist() {
        try {
            this.lock.readLock().lockInterruptibly();
            try {
                KVConfigSerializeWrapper kvConfigSerializeWrapper = new KVConfigSerializeWrapper();
                kvConfigSerializeWrapper.setConfigTable(this.configTable);

                String content = kvConfigSerializeWrapper.toJson();

                if (null != content) {
                    MixAll.string2File(content, this.namesrvController.getNamesrvConfig().getKvConfigPath());
                }
            }
            catch (IOException e) {
                log.error("persist kvconfig Exception, "
                        + this.namesrvController.getNamesrvConfig().getKvConfigPath(), e);
            }
            finally {
                this.lock.readLock().unlock();
            }
        }
        catch (InterruptedException e) {
            log.error("persist InterruptedException", e);
        }

    }


    public void printAllPeriodically() {
        try {
            this.lock.readLock().lockInterruptibly();
            try {
                log.info("--------------------------------------------------------");

                log.info("KVConfigManager {}", this.configTable);
            }
            finally {
                this.lock.readLock().unlock();
            }
        }
        catch (InterruptedException e) {
            log.error("printAllPeriodically InterruptedException", e);
        }
    }
}


class KVConfigSerializeWrapper extends RemotingSerializable {
    private HashMap<String/* Namespace */, HashMap<String/* Key */, String/* Value */>> configTable;


    public HashMap<String, HashMap<String, String>> getConfigTable() {
        return configTable;
    }


    public void setConfigTable(HashMap<String, HashMap<String, String>> configTable) {
        this.configTable = configTable;
    }
}
