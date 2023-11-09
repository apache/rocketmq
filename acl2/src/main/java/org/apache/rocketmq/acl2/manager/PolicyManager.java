package org.apache.rocketmq.acl2.manager;

import java.util.List;
import org.apache.rocketmq.acl2.model.Policy;
import org.apache.rocketmq.acl2.model.Subject;

public interface PolicyManager {

    void createPolicy(Policy policy);

    void deletePolicy(Subject subject);

    void updatePolicy(Policy policy);

    Policy getPolicy(Subject subject);

    List<Policy> listPolicies();
}
