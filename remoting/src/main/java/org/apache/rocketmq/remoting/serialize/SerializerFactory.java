package org.apache.rocketmq.remoting.serialize;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SerializerFactory {

    private static Map<Enum<SerializeType>, Serializer> serializerMap = new ConcurrentHashMap<Enum<SerializeType>, Serializer>();

    private SerializerFactory() {
    }

    static {
        register(SerializeType.JSON, new RemotingSerializable());
        register(SerializeType.ROCKETMQ, new RocketMQSerializable());
        register(SerializeType.MSGPACK, new MsgPackSerializable());
    }

    public static void register(SerializeType type, Serializer serialization) {
        serializerMap.putIfAbsent(type, serialization);
    }

    public static Serializer get(SerializeType type) {
        return serializerMap.get(type);
    }
}
