package org.apache.rocketmq.remoting;

import java.util.Map;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.util.RemotingUtil;
import org.apache.rocketmq.remoting.util.ServiceProvider;

public class RemotingServerFactory {

    private static final InternalLogger log = InternalLoggerFactory.getLogger(RemotingHelper.ROCKETMQ_REMOTING);

    private RemotingServerFactory() {
    }

    private static Map<String, RemotingServer> servers;

//    private static Map<String/*protocolType*/, String/*path*/ >

    private static final String SERVER_LOCATION = "META-INF/service/org.apache.rocketmq.remoting.RemotingServer";

    static {
        log.info("begin load server");
        servers = ServiceProvider.load(SERVER_LOCATION, RemotingClient.class);
        log.info("end load server, size:{}", servers.size());
    }

    public static RemotingServer getRemotingServer() {
        return getRemotingServer(RemotingUtil.DEFAULT_PROTOCOL);
    }

    public static RemotingServer getRemotingServer(String protocolType) {
        return servers.get(protocolType);
    }

//    public static RemotingServer createNewInstance(String protocolType){
//        return ServiceProvider.load()
//    }
}
