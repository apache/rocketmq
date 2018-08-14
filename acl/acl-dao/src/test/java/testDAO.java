import org.apache.rocketmq.acl.dao.domain.AclDO;
import org.apache.rocketmq.acl.dao.manager.BaseDAO;
import org.apache.rocketmq.acl.dao.query.AclQuery;
import org.junit.Test;

import java.util.List;

public class testDAO {
    @Test
    public void testInsert(){
        AclDO aclDO = new AclDO();
        aclDO.setTopic("TestTopic2");
        aclDO.setAppName("TestAppName2");

        Long res = BaseDAO.getInstance().insert(aclDO);
        System.err.println(res);
    }

    @Test
    public void select(){
        AclQuery query = new AclQuery();
        query.setTopic("TestTopic2");
        query.setAppName("TestAppName2");
        List<AclDO> list = BaseDAO.getInstance().select(query);
        System.err.println(list);
    }

    @Test
    public void testUpdate(){
        AclDO aclDO = new AclDO();
        aclDO.setTopic("TestUpdateTopic");
        aclDO.setAppName("TestUpdateAppName");

    }
}
