package org.apache.rocketmq.remoting;

import java.util.Map;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.common.RemotingUtil;
import org.apache.rocketmq.remoting.util.ServiceProvider;

public class RemotingServerFactory {

    private static final InternalLogger log = InternalLoggerFactory.getLogger(RemotingHelper.ROCKETMQ_REMOTING);

    private RemotingServerFactory() {
    }

    private static Map<String, String> protocolPathMap;

    private static final String SERVER_LOCATION = "META-INF/service/org.apache.rocketmq.remoting.RemotingServer";

    static {
        log.info("begin load server");
        protocolPathMap = ServiceProvider.loadPath(SERVER_LOCATION);
        log.info("end load server, size:{}", protocolPathMap.size());
    }


    public static RemotingServer createInstance(String protocol) {
        return ServiceProvider.createInstance(protocolPathMap.get(protocol), RemotingClient.class);
    }
    public static RemotingServer createInstance() {
        return ServiceProvider.createInstance(protocolPathMap.get(RemotingUtil.DEFAULT_PROTOCOL), RemotingServer.class);
    }
}
