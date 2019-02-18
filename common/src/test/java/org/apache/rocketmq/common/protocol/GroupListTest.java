package org.apache.rocketmq.common.protocol;

import org.apache.rocketmq.common.protocol.body.GroupList;
import org.junit.Test;

import java.util.HashSet;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by guoyao on 2019/2/18.
 */
public class GroupListTest {

    @Test
    public void testSetGet() throws Exception {
        HashSet<String> fisrtUniqueSet=createUniqueNewSet();
        HashSet<String> secondUniqueSet=createUniqueNewSet();
        assertThat(fisrtUniqueSet).isNotEqualTo(secondUniqueSet);
        GroupList gl=new GroupList();
        gl.setGroupList(fisrtUniqueSet);
        assertThat(gl.getGroupList()).isEqualTo(fisrtUniqueSet);
        assertThat(gl.getGroupList()).isNotEqualTo(secondUniqueSet);
        gl.setGroupList(secondUniqueSet);
        assertThat(gl.getGroupList()).isNotEqualTo(fisrtUniqueSet);
        assertThat(gl.getGroupList()).isEqualTo(secondUniqueSet);
    }

    private HashSet<String> createUniqueNewSet() {
        HashSet<String> groups=new HashSet<String>();
        groups.add(UUID.randomUUID().toString());
        return groups;
    }
}
