package com.alibaba.rocketmq.broker.digestlog;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


/**
 * 统计管理器
 * 
 * @author boyan
 * @Date 2011-4-22
 * @author wuhua
 * @Date 2011-9-9
 * 
 */
public class DigestLogManager {
    private static final Log log = LogFactory.getLog(DigestLogManager.class);
    private final boolean startRealTimeStat = Boolean.valueOf(System
        .getProperty("meta.realtime.stat", "true"));
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1,
        new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, "DigestLogPrintSchedule");
            }
        });
    private final GetMissStatsMoniter getMissStatsMoniter = new GetMissStatsMoniter();
    private final GetStatsMoniter getStatsMoniter = new GetStatsMoniter();
    private final PutStatsMoniter putStatsMoniter = new PutStatsMoniter();
    private final SlavePutStatsMoniter slavePutStatsMoniter = new SlavePutStatsMoniter();
    private final StoreStatsMoniter storeStatsMoniter = new StoreStatsMoniter();
    private final TransationStatsMoniter transationStatsMoniter = new TransationStatsMoniter();


    public void start() {
        if (startRealTimeStat) {
            scheduler.scheduleWithFixedDelay(new DigestPrintOut(), 10, 20, TimeUnit.SECONDS);
            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    scheduler.shutdown();
                }
            });
            log.warn("实时统计启动...");
        }
    }


    public void dispose() {
        scheduler.shutdown();
    }

    class DigestPrintOut implements Runnable {
        @Override
        public void run() {
            getStatsMoniter.tolog();
            putStatsMoniter.tolog();
            slavePutStatsMoniter.tolog();
            getMissStatsMoniter.tolog();
            storeStatsMoniter.tolog();
            transationStatsMoniter.tolog();
        }
    }


    public GetMissStatsMoniter getGetMissStatsMoniter() {
        return getMissStatsMoniter;
    }


    public GetStatsMoniter getGetStatsMoniter() {
        return getStatsMoniter;
    }


    public PutStatsMoniter getPutStatsMoniter() {
        return putStatsMoniter;
    }


    public StoreStatsMoniter getStoreStatsMoniter() {
        return storeStatsMoniter;
    }


    public TransationStatsMoniter getTransationStatsMoniter() {
        return transationStatsMoniter;
    }


    public SlavePutStatsMoniter getSlavePutStatsMoniter() {
        return slavePutStatsMoniter;
    }
}
