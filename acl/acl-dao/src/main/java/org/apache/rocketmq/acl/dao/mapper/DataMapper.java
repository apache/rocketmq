package org.apache.rocketmq.acl.dao.mapper;

import org.apache.rocketmq.acl.dao.domain.AclDO;
import org.apache.rocketmq.acl.dao.query.AclQuery;

import java.util.List;

public interface DataMapper {

    Long insert(AclDO dataObject);

    Integer update(AclDO dataObject);

    List<AclDO> select(AclQuery query);

    Integer count(AclQuery query);

}
