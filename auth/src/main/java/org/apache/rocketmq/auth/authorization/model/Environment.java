package org.apache.rocketmq.auth.authorization.model;

import java.util.Collections;
import java.util.List;
import org.apache.commons.collections.CollectionUtils;
import org.apache.rocketmq.common.utils.IPAddressUtils;

public class Environment {

    private List<String> sourceIps;

    public static Environment of(String sourceIp) {
        return of(Collections.singletonList(sourceIp));
    }

    public static Environment of(List<String> sourceIps) {
        Environment environment = new Environment();
        environment.setSourceIps(sourceIps);
        return environment;
    }

    public boolean isMatch(Environment environment) {
        if (CollectionUtils.isEmpty(this.sourceIps)) {
            return true;
        }
        if (CollectionUtils.isEmpty(environment.getSourceIps())) {
            return false;
        }
        String targetIp = environment.getSourceIps().get(0);
        for (String sourceIp : this.sourceIps) {
            if (IPAddressUtils.isIPInRange(targetIp, sourceIp)) {
                return true;
            }
        }
        return false;
    }

    public List<String> getSourceIps() {
        return sourceIps;
    }

    public void setSourceIps(List<String> sourceIps) {
        this.sourceIps = sourceIps;
    }
}
