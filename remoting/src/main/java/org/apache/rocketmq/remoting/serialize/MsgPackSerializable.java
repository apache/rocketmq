package org.apache.rocketmq.remoting.serialize;

import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.msgpack.MessagePack;

public class MsgPackSerializable implements Serializer {
    private final MessagePack messagePack = new MessagePack();

//    public MsgPackSerializable(){
//        messagePack.register(LanguageCode.class);
//        messagePack.register(SerializeType.class);
//    }
    @Override
    public SerializeType type() {
        return SerializeType.MSGPACK;
    }

    @Override
    public <T> T deserializer(byte[] content, Class<T> c) {
        try {
            return messagePack.read(content, c);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public RemotingCommand deserializer(byte[] content) {
        return deserializer(content, RemotingCommand.class);
    }

    @Override
    public byte[] serializer(Object object) {
        try {
            return messagePack.write(object);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
