package org.apache.rocketmq.broker.acl.dao;

import ch.qos.logback.core.util.FileUtil;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.rocketmq.broker.acl.domain.AclDomain;
import org.apache.rocketmq.broker.acl.domain.UserDomain;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;

/**
 * @author ycc
 * @date 2018/08/13
 */
public class UserDao {

    public List<UserDomain> readUserInfo() {
        String path = getClass().getClassLoader().getResource("userInfo.json").toString();
        JSONArray jsonArray = null;
        StringBuffer sb = new StringBuffer();
        String line = null;
        try {
            BufferedReader in = new BufferedReader(new FileReader(path));
            line = in.readLine();
            while (line != null) {
                sb.append(line);
            }
            JSONObject jsonObject = JSONObject.parseObject(sb.toString());
            if (jsonObject != null) {
                jsonArray = jsonObject.getJSONArray("list");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        List<UserDomain> result = new ArrayList<>();
        if (jsonArray == null) {
            return result;
        }
        for (int i = 0; i < jsonArray.size(); i++) {
            UserDomain object = (UserDomain)jsonArray.get(i);
            result.add(object);
        }
        return result;
    }

    public boolean isValidate(UserDomain userDomain) {
        List<UserDomain> result = readUserInfo();
        if (result != null && result.contains(userDomain)) {
            return true;
        }
        return false;
    }
}
