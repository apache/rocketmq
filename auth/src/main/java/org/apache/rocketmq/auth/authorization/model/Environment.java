package org.apache.rocketmq.auth.authorization.model;

import java.util.Collections;
import java.util.List;
import org.apache.commons.collections.CollectionUtils;

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
        // TODO 需要校验IP是否在IP段中
        return true;
    }

    public List<String> getSourceIps() {
        return sourceIps;
    }

    public void setSourceIps(List<String> sourceIps) {
        this.sourceIps = sourceIps;
    }
}
