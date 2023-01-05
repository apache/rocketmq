package org.apache.rocketmq.store;

import org.junit.Assert;
import org.junit.Test;

public class GetMessageResultTest {

    @Test
    public void testAddMessage() {
        GetMessageResult getMessageResult = new GetMessageResult();
        SelectMappedBufferResult mappedBufferResult1 = new SelectMappedBufferResult(0, null, 4 * 1024, null);
        getMessageResult.addMessage(mappedBufferResult1);

        SelectMappedBufferResult mappedBufferResult2 = new SelectMappedBufferResult(0, null, 2 * 4 * 1024, null);
        getMessageResult.addMessage(mappedBufferResult2, 0);

        SelectMappedBufferResult mappedBufferResult3 = new SelectMappedBufferResult(0, null, 4 * 4 * 1024, null);
        getMessageResult.addMessage(mappedBufferResult3, 0, 2);

        Assert.assertEquals(getMessageResult.getMessageQueueOffset().size(), 2);
        Assert.assertEquals(getMessageResult.getMessageBufferList().size(), 3);
        Assert.assertEquals(getMessageResult.getMessageMapedList().size(), 3);
        Assert.assertEquals(getMessageResult.getMessageCount(), 4);
        Assert.assertEquals(getMessageResult.getMsgCount4Commercial(), 1 + 2+ 4);
        Assert.assertEquals(getMessageResult.getBufferTotalSize(), (1 + 2+ 4) * 4 * 1024);
    }
}
