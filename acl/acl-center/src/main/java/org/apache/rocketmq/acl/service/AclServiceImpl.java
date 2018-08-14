package org.apache.rocketmq.acl.service;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.rocketmq.acl.AclService;
import org.apache.rocketmq.acl.dao.domain.AclDO;
import org.apache.rocketmq.acl.dao.manager.BaseDAO;
import org.apache.rocketmq.acl.dao.query.AclQuery;

import java.util.List;

public class AclServiceImpl implements AclService {

    @Override
    public boolean aclCheck(String topic, String appName) {
        AclQuery aclQuery = new AclQuery();
        aclQuery.setTopic(topic);
        aclQuery.setAppName(appName);
        List<AclDO> list = BaseDAO.getInstance().select(aclQuery);

        return CollectionUtils.isEmpty(list);
    }

    @Override
    public boolean aclCheck(String topic, String appName, String userInfo) {
        AclQuery aclQuery = new AclQuery();
        aclQuery.setTopic(topic);
        aclQuery.setAppName(appName);
        aclQuery.setUserInfo(userInfo);
        List<AclDO> list = BaseDAO.getInstance().select(aclQuery);

        return CollectionUtils.isEmpty(list);
    }
}
