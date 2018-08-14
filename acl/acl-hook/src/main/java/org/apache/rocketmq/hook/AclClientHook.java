package org.apache.rocketmq.hook;

import org.apache.rocketmq.hook.cache.Cache;
import org.apache.rocketmq.hook.cache.Invoker;
import org.apache.rocketmq.hook.cache.MemoryCache;
import org.apache.rocketmq.hook.exception.AclFailException;
import org.apache.rocketmq.hook.rpc.AclService;
import org.apache.rocketmq.hook.rpc.impl.AclServiceHttpProxy;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

public class AclClientHook implements RPCHook {
    private String info;
    private String appName;
    private String topic;
    private Cache cache = MemoryCache.getInstance();
    private AbstractKeyGenerator kg = new KeyGenerator();
    private AclService acl_center;

    private String acl_host = "127.0.0.1";
    private int acl_port = 8080;

    private boolean isAcl = false;

    public AclClientHook(String info) {
        this.info = info;
    }

    public AclClientHook(String appName, String topic, AclService aclService) {
        this.appName = appName;
        this.topic = topic;
        this.acl_center = AclServiceHttpProxy.getInstance(acl_host, acl_port);
    }

    @Override
    public void doBeforeRequest(String remoteAddr, RemotingCommand request) {
        Boolean isVailtd = (Boolean) cache.getObject(kg.genKey(appName, topic),  new Invoker<Boolean>() {
            @Override
            public Boolean invoke() {
                boolean b = acl_center.aclCheck(topic, appName);
                return b;
            }
        }, 5);
        this.isAcl = isVailtd;
    }

    @Override
    public void doAfterResponse(String remoteAddr, RemotingCommand request, RemotingCommand response) {
        // do nothing.
    }

    class KeyGenerator implements AbstractKeyGenerator {
        @Override
        public String genKey(String appName, String topic) {
            return appName + "_" + topic;
        }
    }

    public boolean getIsAcl() {
        return this.isAcl;
    }

    public void checkAcl() throws AclFailException {
        if (!this.getIsAcl()) {
            throw new AclFailException("can't pass acl.");
        }
    }
}