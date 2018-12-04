package org.apache.rocketmq.store.dledger;

import java.util.UUID;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.junit.Assert;
import org.junit.Test;

public class MixCommitlogTest extends DLedgerCommitlogTest {


    @Test
    public void testPutAndGet() throws Exception {
        String base =  createBaseDir();
        String topic = UUID.randomUUID().toString();
        String peers = String.format("n0-localhost:%d", nextPort());
        String group = UUID.randomUUID().toString();

        long dividedOffset;
        {
            DefaultMessageStore originalStore = createMessageStore(base, false);
            doPutMessages(originalStore, topic, 0, 1000, 0);
            Thread.sleep(500);
            Assert.assertEquals(0, originalStore.getMinOffsetInQueue(topic, 0));
            Assert.assertEquals(1000, originalStore.getMaxOffsetInQueue(topic, 0));
            Assert.assertEquals(0, originalStore.dispatchBehindBytes());
            dividedOffset = originalStore.getCommitLog().getMaxOffset();
            dividedOffset = dividedOffset - dividedOffset % originalStore.getMessageStoreConfig().getMapedFileSizeCommitLog() + originalStore.getMessageStoreConfig().getMapedFileSizeCommitLog();
            originalStore.shutdown();
        }
        {
            DefaultMessageStore recoverOriginalStore = createMessageStore(base, true);
            Thread.sleep(500);
            Assert.assertEquals(0, recoverOriginalStore.getMinOffsetInQueue(topic, 0));
            Assert.assertEquals(1000, recoverOriginalStore.getMaxOffsetInQueue(topic, 0));
            Assert.assertEquals(0, recoverOriginalStore.dispatchBehindBytes());
            recoverOriginalStore.shutdown();
        }
        {
            DefaultMessageStore dledgerStore = createDledgerMessageStore(base, group, "n0", peers, null, true);
            DLedgerCommitLog dLedgerCommitLog = (DLedgerCommitLog) dledgerStore.getCommitLog();
            Assert.assertEquals(dividedOffset, dLedgerCommitLog.getDividedCommitlogOffset());
            Thread.sleep(2000);
            doPutMessages(dledgerStore, topic, 0, 1000, 1000);
            Thread.sleep(500);
            Assert.assertEquals(0, dledgerStore.getMinOffsetInQueue(topic, 0));
            Assert.assertEquals(2000, dledgerStore.getMaxOffsetInQueue(topic, 0));
            Assert.assertEquals(0, dledgerStore.dispatchBehindBytes());
            dledgerStore.shutdown();
        }

    }
}
