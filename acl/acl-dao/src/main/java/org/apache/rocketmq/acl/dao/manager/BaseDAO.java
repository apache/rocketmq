package org.apache.rocketmq.acl.dao.manager;

import org.apache.ibatis.session.SqlSession;
import org.apache.rocketmq.acl.dao.domain.AclDO;
import org.apache.rocketmq.acl.dao.mapper.DataMapper;
import org.apache.rocketmq.acl.dao.query.AclQuery;

import java.util.List;

/**
 * 基础dao
 * @author chjie.gcj
 * @date 2018-08-13
 */
public class BaseDAO {
    private static BaseDAO instance=new BaseDAO();
    private BaseDAO(){ }
    public static BaseDAO getInstance(){
        return instance;
    }

    private SqlSession session;
    private DataMapper mapper;

    private void init(){
        session = DBtool.getSession();
        mapper = session.getMapper(DataMapper.class);
    }
    private void commit(){
        session.commit();
    }
    private void close(){
        session.close();
    }

    public List<AclDO> select(AclQuery query){
        this.init();
        try {
            List<AclDO> info=mapper.select(query);
            this.commit();
            return info;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }finally {
            this.close();
        }
    }

    public Long insert(AclDO aclDO){
        this.init();
        try {
            aclDO.setRowStatus(0);
            aclDO.setRowVersion(1L);
            Long id = mapper.insert(aclDO);
            this.commit();
            return id;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }finally {
            this.close();
        }
    }

    public Integer delete(){
        this.init();
        try {
            AclDO aclDO = new AclDO();
            aclDO.setRowStatus(-1);
            Integer res = mapper.update(aclDO);
            this.commit();
            return res;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }finally {
            this.close();
        }
    }




}
