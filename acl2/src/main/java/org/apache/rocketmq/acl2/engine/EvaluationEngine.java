package org.apache.rocketmq.acl2.engine;

import java.util.List;
import org.apache.rocketmq.acl2.enums.Decision;
import org.apache.rocketmq.acl2.model.PolicyEntry;
import org.apache.rocketmq.acl2.model.RequestContext;
import org.apache.rocketmq.acl2.model.Subject;

public interface EvaluationEngine {

    void evaluate(RequestContext context);
}
