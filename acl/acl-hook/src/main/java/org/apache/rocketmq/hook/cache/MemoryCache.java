package org.apache.rocketmq.hook.cache;

import java.util.Date;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MemoryCache extends AbstractCache {
    private final static Map<String,Object> DATA = new ConcurrentHashMap<>();
    private final static Map<String,Long> EXPIRE = new ConcurrentHashMap<>();

    private long cleanIntervalTimeSecond = 15;

    @Override
    public synchronized boolean exist(String key) {
        Object o = DATA.get(key);
        if(o != null){
            return !isExpire(key);
        }else{
            return false;
        }
    }

    private synchronized boolean isExpire(String key){
        Long expireTime = EXPIRE.get(key);
        if(expireTime == null){
            //if null,then it will not set expire time
            return false;
        }else{
            if(System.currentTimeMillis() > expireTime){
                //expire
                DATA.remove(key);
                EXPIRE.remove(key);
                return true;
            }else{
                return false;
            }
        }
    }

    @Override
    public synchronized void delete(String key) {
        DATA.remove(key);
        EXPIRE.remove(key);
    }

    @Override
    public synchronized void expire(String key, long millisecond) {
        if(DATA.get(key) != null){
            EXPIRE.put(key,System.currentTimeMillis() + millisecond);
        }
    }

    @Override
    public synchronized void set(String key, Object value) {
        DATA.put(key,value);
    }

    @Override
    public synchronized <T> T getObject(String key) {
        if(exist(key)){
            return (T) DATA.get(key);
        }else{
            return null;
        }

    }

    @Override
    public synchronized String getString(String key) {
        if(exist(key)){
            return String.valueOf(DATA.get(key));
        }else{
            return null;
        }

    }

    @Override
    public synchronized Integer getInt(String key) {
        if(exist(key)){
            return (Integer) DATA.get(key);
        }else{
            return null;
        }

    }

    @Override
    public synchronized Long getLong(String key) {
        if(exist(key)){
            return (Long) DATA.get(key);
        }else{
            return null;
        }

    }

    @Override
    public synchronized Double getDouble(String key) {
        if(exist(key)){
            return (Double) DATA.get(key);
        }else {
            return null;
        }

    }

    @Override
    public synchronized Date getDate(String key) {
        if(exist(key)){
            return (Date) DATA.get(key);
        }else {
            return null;
        }

    }

    public synchronized void flushDb() {
        DATA.clear();
        EXPIRE.clear();
    }

    public MemoryCache(){
        this.startCleaner();
    }

    protected void startCleaner(){
        new Thread(new Runnable() {
            @Override
            public void run() {
                while (true){
                    try {
                        Thread.sleep(cleanIntervalTimeSecond * 1000);
                        for(String key : DATA.keySet()){
                            if(isExpire(key)){
                                delete(key);
                            }
                        }


                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }

            }
        }).start();
    }


    public long getCleanIntervalTimeSecond() {
        return cleanIntervalTimeSecond;
    }

    public void setCleanIntervalTimeSecond(long cleanIntervalTimeSecond) {
        this.cleanIntervalTimeSecond = cleanIntervalTimeSecond;
    }

    public static Cache getInstance(){
        return Instance.cache;
    }


    private static class Instance{
        private static Cache cache = new MemoryCache();
    }

    public static void main(String[] args) {
        final String key = "ni";
        Boolean val = false;

        Cache instance = MemoryCache.getInstance();
        Boolean object = instance.getObject(key, new Invoker<Boolean>() {
            @Override
            public Boolean invoke() {
                return true;
            }
        }, 1);

        System.out.println(object);
    }
}