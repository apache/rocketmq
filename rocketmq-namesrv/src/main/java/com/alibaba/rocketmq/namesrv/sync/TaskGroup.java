package com.alibaba.rocketmq.namesrv.sync;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;


/**
 * 
 * @author lansheng.zj@taobao.com
 *
 * @param <R> 返回值类型
 * @param <T> 参数类型
 */
public class TaskGroup<R, T> implements Iterable<Entry<String, Task<R, T>>> {

    private Map<String, Task<R, T>> taskMap;


    public TaskGroup() {
        this.taskMap = new HashMap<String, Task<R, T>>();
    }


    public Task<R, T> addTask(String key, Exec<R, T> task) {
        return taskMap.put(key, new Task<R, T>(task));
    }


    public Task<R, T> getTask(String key) {
        return taskMap.get(key);
    }


    public Task<R, T> removeTask(String key) {
        return taskMap.remove(key);
    }


    public int size() {
        return taskMap.size();
    }
    
    
    public Set<String> getKeys() {
        return taskMap.keySet();
    }


    @Override
    public Iterator<Entry<String, Task<R, T>>> iterator() {
        return new Iterator<Entry<String, Task<R, T>>>() {

            private Iterator<Entry<String, Task<R, T>>> iterator = taskMap.entrySet().iterator();


            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }


            @Override
            public Entry<String, Task<R, T>> next() {
                return iterator.next();
            }


            @Override
            public void remove() {
                iterator.remove();
            }
        };
    }
}
