package org.apache.rocketmq.remoting.protocol;

import org.junit.Test;

public class SerializeTypeTest {

    @Test(expected = IllegalArgumentException.class)
    public void testSerializeTypeException() {
        // the right code
        byte typeCode1 = (byte) 1;

        // the wrong code
        byte typeCode2 = (byte) 3;

        SerializeType protocolType1 = SerializeType.valueOf(typeCode1);
        System.out.println(protocolType1);

        SerializeType serializeType2 = SerializeType.valueOf(typeCode2);
        System.out.println(serializeType2);
    }


}