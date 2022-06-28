package org.apache.rocketmq.store.kv;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.*;

public class CompactionPositionMgrTest {

    @Rule
    public TemporaryFolder tmpFolder = new TemporaryFolder();

    File file;

    @Before
    public void setUp() throws IOException  {
        file = tmpFolder.newFolder("compaction");
        System.out.println(file.getAbsolutePath());
    }

    @Test
    public void testGetAndSet() {
        CompactionPositionMgr mgr = new CompactionPositionMgr(file.getAbsolutePath());
        mgr.setOffset("topic1", 1, 1);
        assertEquals(1, mgr.getOffset("topic1", 1));
        mgr.setOffset("topic1", 1, 2);
        assertEquals(2, mgr.getOffset("topic1", 1));
        mgr.setOffset("topic1", 2, 1);
        assertEquals(1, mgr.getOffset("topic1", 2));
    }

    @Test
    public void testLoadAndPersist() throws IOException {
        CompactionPositionMgr mgr = new CompactionPositionMgr(file.getAbsolutePath());
        mgr.setOffset("topic1", 1, 2);
        mgr.setOffset("topic1", 2, 1);
        mgr.persist();
        mgr = null;

        CompactionPositionMgr mgr2 = new CompactionPositionMgr(file.getAbsolutePath());
        mgr2.load();
        assertEquals(2, mgr2.getOffset("topic1", 1));
        assertEquals(1, mgr2.getOffset("topic1", 2));
    }
}