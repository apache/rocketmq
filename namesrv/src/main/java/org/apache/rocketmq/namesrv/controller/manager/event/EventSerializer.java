package org.apache.rocketmq.namesrv.controller.manager.event;

import org.apache.commons.lang3.SerializationException;
import org.apache.rocketmq.common.utils.FastJsonSerializer;

/**
 * @author hzh
 * @email 642256541@qq.com
 * @date 2022/4/21 16:11
 */
public class EventSerializer {
    private final FastJsonSerializer serializer;

    public  EventSerializer() {
        this.serializer = new FastJsonSerializer();
    }

    private void putShort(byte[] memory, int index, int value) {
        memory[index] = (byte) (value >>> 8);
        memory[index + 1] = (byte) value;
    }

    private short getShort(byte[] memory, int index) {
        return (short) (memory[index] << 8 | memory[index + 1] & 0xFF);
    }

    public byte[] serialize(EventMessage message) throws SerializationException {
        final short eventType = message.getEventType().getId();
        final byte[] data = this.serializer.serialize(message);
        if (data != null && data.length > 0) {
            final byte[] result = new byte[2 + data.length];
            putShort(result, 0, eventType);
            System.arraycopy(data, 0, result, 2, data.length);
            return result;
        }
        return null;
    }

    public EventMessage deserialize(byte[] bytes) throws SerializationException {
        if (bytes.length < 2) {
            return null;
        }
        final short eventId = getShort(bytes, 0);
        if (eventId > 0) {
            final byte[] data = new byte[bytes.length - 2];
            System.arraycopy(bytes, 2, data, 0, data.length);
            final EventType eventType = EventType.from(eventId);
            if (eventType != null) {
                switch (eventType) {
                    case ALTER_SYNC_STATE_SET_EVENT:
                        return this.serializer.deserialize(data, AlterSyncStateSetEvent.class);
                    case APPLY_BROKER_ID_EVENT:
                        return this.serializer.deserialize(data, ApplyBrokerIdEvent.class);
                    case ELECT_MASTER_EVENT:
                        return this.serializer.deserialize(data, ElectMasterEvent.class);
                    default:
                        break;
                }
            }
        }
        return null;
    }
}
