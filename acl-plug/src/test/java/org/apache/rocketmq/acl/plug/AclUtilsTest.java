package org.apache.rocketmq.acl.plug;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class AclUtilsTest {

    @Test
    public void getAddreeStrArray() {
        String address = "1.1.1.{1,2,3,4}";
        String[] addressArray = AclUtils.getAddreeStrArray(address, "{1,2,3,4}");
        List<String> newAddressList = new ArrayList<>();
        for (String a : addressArray) {
            newAddressList.add(a);
        }

        List<String> addressList = new ArrayList<>();
        addressList.add("1.1.1.1");
        addressList.add("1.1.1.2");
        addressList.add("1.1.1.3");
        addressList.add("1.1.1.4");
        Assert.assertEquals(newAddressList, addressList);
    }

    @Test
    public void isScopeStringArray() {
        String adderss = "12";

        for (int i = 0; i < 6; i++) {
            boolean isScope = AclUtils.isScope(adderss, 4);
            if (i == 3) {
                Assert.assertTrue(isScope);
            } else {
                Assert.assertFalse(isScope);
            }
            adderss = adderss + ".12";
        }
    }

    @Test
    public void isScopeArray() {
        String[] adderss = StringUtils.split("12.12.12.12", ".");
        boolean isScope = AclUtils.isScope(adderss, 4);
        Assert.assertTrue(isScope);
        isScope = AclUtils.isScope(adderss, 3);
        Assert.assertTrue(isScope);

        adderss = StringUtils.split("12.12.1222.1222", ".");
        isScope = AclUtils.isScope(adderss, 4);
        Assert.assertFalse(isScope);
        isScope = AclUtils.isScope(adderss, 3);
        Assert.assertFalse(isScope);

    }

    @Test
    public void isScopeStringTest() {
        for (int i = 0; i < 256; i++) {
            boolean isScope = AclUtils.isScope(i + "");
            Assert.assertTrue(isScope);
        }
        boolean isScope = AclUtils.isScope("-1");
        Assert.assertFalse(isScope);
        isScope = AclUtils.isScope("256");
        Assert.assertFalse(isScope);
    }

    @Test
    public void isScopeTest() {
        for (int i = 0; i < 256; i++) {
            boolean isScope = AclUtils.isScope(i);
            Assert.assertTrue(isScope);
        }
        boolean isScope = AclUtils.isScope(-1);
        Assert.assertFalse(isScope);
        isScope = AclUtils.isScope(256);
        Assert.assertFalse(isScope);

    }

    @Test
    public void isAsteriskTest() {
        boolean isAsterisk = AclUtils.isAsterisk("*");
        Assert.assertTrue(isAsterisk);

        isAsterisk = AclUtils.isAsterisk(",");
        Assert.assertFalse(isAsterisk);
    }

    @Test
    public void isColonTest() {
        boolean isColon = AclUtils.isColon(",");
        Assert.assertTrue(isColon);

        isColon = AclUtils.isColon("-");
        Assert.assertFalse(isColon);
    }

    @Test
    public void isMinusTest() {
        boolean isMinus = AclUtils.isMinus("-");
        Assert.assertTrue(isMinus);

        isMinus = AclUtils.isMinus("*");
        Assert.assertFalse(isMinus);
    }
}
