package org.apache.rocketmq.broker.acl.serviceImpl;

import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.broker.acl.dao.AclDao;
import org.apache.rocketmq.broker.acl.dao.UserDao;
import org.apache.rocketmq.broker.acl.domain.AclDomain;
import org.apache.rocketmq.broker.acl.domain.OpsEnum;
import org.apache.rocketmq.broker.acl.domain.UserDomain;
import org.apache.rocketmq.broker.acl.service.AclService;

import java.util.HashSet;
import java.util.Set;

/**
 *
 *@author ycc
 *@date 2018/08/13
 */
public class AclServiceImpl implements AclService {

    private UserDao userDao = new UserDao();

    @Override
    public boolean createAcl(String topicId, String groupId, String userId, String password) {
        if(!userDao.isValidate(new UserDomain(Long.parseLong(userId),password)))return false;
        AclDomain aclDomain = new AclDomain(topicId,groupId,new HashSet<String>(), OpsEnum.AUTH_OWNER,0);

        return AclDao.add(aclDomain);
    }

    @Override
    public boolean addReadAcl(String userId,String password,String topicId,String targetUserId) {
        if(!userDao.isValidate(new UserDomain(Long.parseLong(userId),password)))return false;
        if(!isOwner(topicId,userId))return false;
        String key = OpsEnum.AUTH_READ.getCode()+","+topicId;
        AclDomain tmp = TopicConfig.aclMap.get(key);
        tmp.getUserIds().add(userId);
        return AclDao.update(tmp);
    }

    @Override
    public boolean addWriteAcl(String userId,String password,String topicId,String targetUserId) {
        if(!userDao.isValidate(new UserDomain(Long.parseLong(userId),password)))return false;
        if(!isOwner(topicId,userId))return false;
        String key = OpsEnum.AUTH_WRITE.getCode()+","+topicId;
        AclDomain tmp = TopicConfig.aclMap.get(key);
        tmp.getUserIds().add(userId);
        return AclDao.update(tmp);
    }

    @Override
    public boolean delReadAcl(String userId,String password,String topicId, String targetUserId) {
        if(!userDao.isValidate(new UserDomain(Long.parseLong(userId),password)))return false;
        if(!isOwner(topicId,userId))return false;
        String key = OpsEnum.AUTH_READ.getCode()+","+topicId;
        AclDomain tmp = TopicConfig.aclMap.get(key);
        tmp.getUserIds().remove(userId);
        return AclDao.update(tmp);
    }

    @Override
    public boolean delWriteAcl(String userId,String password,String topicId, String targetUserId) {
        if(!userDao.isValidate(new UserDomain(Long.parseLong(userId),password)))return false;
        if(!isOwner(topicId,userId))return false;
        String key = OpsEnum.AUTH_WRITE.getCode()+","+topicId;
        AclDomain tmp = TopicConfig.aclMap.get(key);
        tmp.getUserIds().remove(userId);
        return AclDao.update(tmp);
    }


    @Override
    public boolean delTopic(String userId,String password,String topicId) {
        if(!userDao.isValidate(new UserDomain(Long.parseLong(userId),password)))return false;
        if(!isOwner(topicId,userId))return false;
        String rKey = OpsEnum.AUTH_READ.getCode()+","+topicId;
        AclDomain rTmp = TopicConfig.aclMap.get(rKey);
        rTmp.setStatus(-1);
        String wKey = OpsEnum.AUTH_WRITE.getCode()+","+topicId;
        AclDomain wTmp = TopicConfig.aclMap.get(wKey);
        wTmp.setStatus(-1);
        String oKey = OpsEnum.AUTH_OWNER.getCode()+","+topicId;
        AclDomain oTmp = TopicConfig.aclMap.get(oKey);
        oTmp.setStatus(-1);
        return AclDao.update(rTmp)&AclDao.update(wTmp)&AclDao.update(oTmp);
    }

    @Override
    public Set<String> getAllReadUserId(String topicId) {
        String rKey = OpsEnum.AUTH_READ.getCode()+","+topicId;
        AclDomain rTmp = TopicConfig.aclMap.get(rKey);
        return rTmp.getUserIds();
    }

    @Override
    public Set<String> getAllWriteUserId(String topicId) {
        String wKey = OpsEnum.AUTH_WRITE.getCode()+","+topicId;
        AclDomain wTmp = TopicConfig.aclMap.get(wKey);
        return wTmp.getUserIds();
    }

    @Override
    public String getAllOwnerUserId(String topicId) {
        String oKey = OpsEnum.AUTH_OWNER.getCode()+","+topicId;
        AclDomain oTmp = TopicConfig.aclMap.get(oKey);
        Set<String> res = oTmp.getUserIds();
        if(res.isEmpty())return null;
        return res.iterator().next();
    }

    @Override
    public boolean canRead(String topicId, String userId) {
        String rKey = OpsEnum.AUTH_READ.getCode()+","+topicId;
        AclDomain rTmp = TopicConfig.aclMap.get(rKey);
        Set<String> res = rTmp.getUserIds();
        if(res.isEmpty())return false;
        return res.contains(userId);
    }

    @Override
    public boolean canWrite(String topicId, String userId) {
        String wKey = OpsEnum.AUTH_WRITE.getCode()+","+topicId;
        AclDomain wTmp = TopicConfig.aclMap.get(wKey);
        Set<String> res = wTmp.getUserIds();
        if(res.isEmpty())return false;
        return res.contains(userId);
    }

    @Override
    public boolean isOwner(String topicId, String userId) {
        String oKey = OpsEnum.AUTH_OWNER.getCode()+","+topicId;
        AclDomain oTmp = TopicConfig.aclMap.get(oKey);
        Set<String> res = oTmp.getUserIds();
        if(res.isEmpty())return false;
        return res.contains(userId);
    }
}
