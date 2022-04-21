package org.apache.rocketmq.common.utils;

import org.apache.commons.lang3.SerializationException;

/**
 * Serializer
 */
public interface Serializer {

    /**
     * Serialize object t to byte[]
     */
    <T> byte[] serialize(T t) throws SerializationException;

    /**
     * De-serialize bytes to T
     */
    <T> T deserialize(byte[] bytes, Class<T> type) throws SerializationException;
}
