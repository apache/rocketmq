package org.apache.rocketmq.acl.common;

import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import java.lang.reflect.Field;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.rocketmq.acl.common.SessionCredentials.AccessKey;
import static org.apache.rocketmq.acl.common.SessionCredentials.SecurityToken;
import static org.apache.rocketmq.acl.common.SessionCredentials.Signature;

public class AclClientRPCHook implements RPCHook {
    protected ConcurrentHashMap<Class<? extends CommandCustomHeader>, Field[]> fieldCache =
            new ConcurrentHashMap<Class<? extends CommandCustomHeader>, Field[]>();



    private final SessionCredentials sessionCredentials;

    public AclClientRPCHook(SessionCredentials sessionCredentials) {
        this.sessionCredentials = sessionCredentials;
    }

    @Override
    public void doBeforeRequest(String remoteAddr, RemotingCommand request) {
        byte[] total = AclUtils.combineRequestContent(request,
            parseRequestContent(request, sessionCredentials.getAccessKey(), sessionCredentials.getSecurityToken()));
        String signature = AclUtils.calSignature(total, sessionCredentials.getSecretKey());
        request.addExtField(Signature, signature);
        request.addExtField(AccessKey, sessionCredentials.getAccessKey());

        if (sessionCredentials.getSecurityToken() != null) {
            request.addExtField(SecurityToken, sessionCredentials.getSecurityToken());
        }
    }


    @Override
    public void doAfterResponse(String remoteAddr, RemotingCommand request, RemotingCommand response) {

    }

    protected SortedMap<String, String> parseRequestContent(RemotingCommand request, String ak, String securityToken) {
        CommandCustomHeader header = request.readCustomHeader();
        // sort property
        SortedMap<String, String> map = new TreeMap<String, String>();
        map.put(AccessKey, ak);
        if (securityToken != null) {
            map.put(SecurityToken, securityToken);
        }
        try {
            // add header properties
            if (null != header) {
                Field[] fields = fieldCache.get(header.getClass());
                if (null == fields) {
                    fields = header.getClass().getDeclaredFields();
                    for (Field field : fields) {
                        field.setAccessible(true);
                    }
                    Field[] tmp = fieldCache.putIfAbsent(header.getClass(), fields);
                    if (null != tmp) {
                        fields = tmp;
                    }
                }

                for (Field field : fields) {
                    Object value = field.get(header);
                    if (null != value && !field.isSynthetic()) {
                        map.put(field.getName(), value.toString());
                    }
                }
            }
            return map;
        } catch (Exception e) {
            throw new RuntimeException("incompatible exception.", e);
        }
    }

    public SessionCredentials getSessionCredentials() {
        return sessionCredentials;
    }
}
