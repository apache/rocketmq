package org.apache.rocketmq.acl.common;

import org.junit.Test;

public class AclSignerTest {

    @Test(expected = Exception.class)
    public void calSignatureExceptionTest(){
        AclSigner.calSignature(new byte[]{},"");
    }

    @Test
    public void calSignatureTest(){
        AclSigner.calSignature("RocketMQ","12345678");
        AclSigner.calSignature("RocketMQ".getBytes(),"12345678");
    }

}
