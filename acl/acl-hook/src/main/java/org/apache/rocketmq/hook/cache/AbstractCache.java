package org.apache.rocketmq.hook.cache;

public abstract class AbstractCache implements Cache {

    @Override
    public <T> T getObject(String key, Invoker<T> invoker, int second) {
        T object = this.getObject(key);
        if(object != null){
            if(NULL.equals(object)){
                return null;
            }else{
                return object;
            }
        }else{
            object = invoker.invoke();
            if(object == null){
                object = (T) NULL;
            }
            this.set(key,object);
            this.expire(key,second * 1000);
            if(NULL.equals(object)){
                return null;
            }else{
                return object;
            }
        }
    }
}