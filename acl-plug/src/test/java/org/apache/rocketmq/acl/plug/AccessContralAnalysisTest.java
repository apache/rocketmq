package org.apache.rocketmq.acl.plug;

import java.util.Map;

import org.apache.rocketmq.acl.plug.entity.BorkerAccessControl;
import org.junit.Test;

public class AccessContralAnalysisTest {

    @Test
    public void analysisTest() {
        AccessContralAnalysis accessContralAnalysis = new AccessContralAnalysis();
        Map<Integer, Boolean> map = accessContralAnalysis.analysis(new BorkerAccessControl());
        System.out.println(map);
    }

}
