package org.apache.rocketmq.tieredstore.provider.posix;

import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.UUID;
import org.apache.commons.io.FileUtils;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.tieredstore.common.TieredMessageStoreConfig;
import org.apache.rocketmq.tieredstore.provider.TieredFileSegment;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class PosixFileSegmentTest {
    TieredMessageStoreConfig storeConfig;
    MessageQueue mq;

    @Before
    public void setUp() {
        storeConfig = new TieredMessageStoreConfig();
        storeConfig.setTieredStoreFilepath(FileUtils.getTempDirectory() + File.separator + "tiered_store_unit_test" + UUID.randomUUID());
        mq = new MessageQueue("OSSFileSegmentTest", "broker", 0);
    }

    @After
    public void tearDown() throws IOException {
        FileUtils.deleteDirectory(new File(FileUtils.getTempDirectory() + File.separator + "tiered_store_unit_test" + UUID.randomUUID()));
    }

    @Test
    public void testCommitAndRead() throws IOException {
        PosixFileSegment fileSegment = new PosixFileSegment(TieredFileSegment.FileSegmentType.CONSUME_QUEUE, mq, 0, storeConfig);
        byte[] source = new byte[4096];
        new Random().nextBytes(source);
        ByteBuffer buffer = ByteBuffer.wrap(source);
        fileSegment.append(buffer, 0);
        fileSegment.commit();

        File file = new File(fileSegment.getPath());
        Assert.assertTrue(file.exists());
        byte[] result = new byte[4096];
        ByteStreams.read(Files.asByteSource(file).openStream(), result, 0, 4096);
        Assert.assertArrayEquals(source, result);

        ByteBuffer read = fileSegment.read(0, 4096);
        Assert.assertArrayEquals(source, read.array());
    }
}
