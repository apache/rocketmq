package org.apache.rocketmq.remoting.protocol;

import org.junit.Assert;
import org.junit.Test;

public class SerializeTypeTest {

    @Test(expected = IllegalArgumentException.class)
    public void testSerializeType() {
        // the right code
        byte typeCode1 = (byte) 0;
        byte typeCode2 = (byte) 1;
        // the wrong code
        byte typeCode3 = (byte) 2;

        Assert.assertEquals(SerializeType.valueOf(typeCode1), SerializeType.JSON);
        Assert.assertEquals(SerializeType.valueOf(typeCode2), SerializeType.ROCKETMQ);

        // will cause Exception
        SerializeType.valueOf(typeCode3);
    }
}
