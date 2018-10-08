package org.apache.rocketmq.acl.plug;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.rocketmq.acl.plug.entity.BorkerAccessControl;
import org.junit.Assert;
import org.junit.Test;

public class AccessContralAnalysisTest {

    @Test
    public void analysisTest() {
        AccessContralAnalysis accessContralAnalysis = new AccessContralAnalysis();
        BorkerAccessControl accessControl = new BorkerAccessControl();
        accessControl.setSendMessage(false);
        Map<Integer, Boolean> map = accessContralAnalysis.analysis(accessControl);

        Iterator<Entry<Integer, Boolean>> it = map.entrySet().iterator();
        long num = 0;
        while (it.hasNext()) {
            Entry<Integer, Boolean> e = it.next();
            if (!e.getValue()) {
                Assert.assertEquals(e.getKey(), Integer.valueOf(10));
                num++;
            }
        }
        Assert.assertEquals(num, 1);

    }

}
