package org.apache.rocketmq.remoting.protocol;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class SerializeTypeTest {

    @Test(expected = IllegalArgumentException.class)
    public void testSerializeTypeException() {
        byte typeCode1 = (byte) 0;
        byte typeCode2 = (byte) 1;
        // the wrong code
        byte typeCode3 = (byte) 2;

        assertThat(SerializeType.valueOf(typeCode1).equals(SerializeType.JSON));
        assertThat(SerializeType.valueOf(typeCode2).equals(SerializeType.ROCKETMQ));

        // will cause Exception
        SerializeType.valueOf(typeCode3);
    }

    @Test
    public void testSerializeTypeGetCode(){
        SerializeType json = SerializeType.JSON;
        SerializeType rocketmq = SerializeType.ROCKETMQ;

        assertThat(json.getCode()).isEqualTo((byte) 0);
        assertThat(rocketmq.getCode()).isEqualTo((byte) 1);
    }


}