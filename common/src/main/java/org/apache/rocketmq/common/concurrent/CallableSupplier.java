package org.apache.rocketmq.common.concurrent;

import java.util.ArrayList;
import java.util.Collection;
import java.util.function.Supplier;

/**
 * 弥补原生{@link Supplier}没有回调用的痛点
 *
 * @author zhangzicheng
 * @date 2021/05/12
 */
public interface CallableSupplier<T> extends Supplier<T> {

    default Callback<T> getCallback() {
        return null;
    }

    default Collection<Callback<T>> getCallbacks() {
        ArrayList<Callback<T>> callbacks = new ArrayList<>();
        Callback<T> callback = getCallback();
        if (callback != null) {
            callbacks.add(callback);
        }
        return callbacks;
    }

    default boolean addCallback(Callback<T> callback) {
        Collection<Callback<T>> callbacks = getCallbacks();
        if (null == callbacks) {
            return false;
        }
        return callbacks.add(callback);
    }

    default boolean removeCallback(Callback<T> callback) {
        Collection<Callback<T>> callbacks = getCallbacks();
        if (null == callbacks) {
            return true;
        }
        return callbacks.remove(callback);
    }
}
