package org.apache.rocketmq.acl.common;

import org.junit.Test;

public class AclSignerTest {

    @Test
    public void calSignatureExceptionTest(){
        try {
            AclSigner.calSignature(new byte[]{},"");
        }catch (Exception e){

        }
    }

    @Test
    public void calSignatureTest(){
        AclSigner.calSignature("RocketMQ","12345678");
        AclSigner.calSignature("RocketMQ".getBytes(),"12345678");
    }

}
