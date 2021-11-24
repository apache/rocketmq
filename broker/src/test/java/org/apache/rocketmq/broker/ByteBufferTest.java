package org.apache.rocketmq.broker;

import org.junit.Test;

import java.nio.ByteBuffer;

/**
 * @author ：zhangqiang
 * @date ：Created in 2021/9/28 11:31
 * @description：
 * @modified By：
 * @version:
 */
public class ByteBufferTest {
    @Test
    public void  testBuffer(){
        ByteBuffer byteBuffer=ByteBuffer.allocate(20);
        print(byteBuffer);
        byteBuffer.put((byte)1);
        byteBuffer.put((byte)2);
        print(byteBuffer);
        byteBuffer.mark();
        byteBuffer.put((byte)3);
      //  byteBuffer.reset();
        byteBuffer.flip();
        byteBuffer.put((byte)4);
       //这里还有默认值
       byte value= byteBuffer.get(3);
        System.out.println(value);
//        System.out.println("**********************flip");
//        byteBuffer.flip();
//        print(byteBuffer);
//        System.out.println(   byteBuffer.get());
//        print(byteBuffer);

    }



    private void print(ByteBuffer byteBuffer){
        System.out.printf("position: %d, limit: %d, capacity: %d\n",byteBuffer.position(),byteBuffer.limit(),
                byteBuffer.capacity());
    }
}
