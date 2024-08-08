package org.apache.rocketmq.acl.common;

import org.junit.Before;
import org.junit.Test;
import org.junit.Assert;

public class AuthorizationHeaderTest {

    private static final String AUTH_HEADER = "Signature Credential=1234567890/test, SignedHeaders=host, Signature=1234567890";
    private AuthorizationHeader authorizationHeader;

    @Before
    public void setUp() throws Exception {
        authorizationHeader = new AuthorizationHeader(AUTH_HEADER);
    }

    @Test
    public void testGetMethod() {
        Assert.assertEquals("Signature", authorizationHeader.getMethod());
    }

    @Test
    public void testGetAccessKey() {
        Assert.assertEquals("1234567890", authorizationHeader.getAccessKey());
    }

    @Test
    public void testGetSignedHeaders() {
        String[] expectedHeaders = {"host"};
        Assert.assertArrayEquals(expectedHeaders, authorizationHeader.getSignedHeaders());
    }

    @Test
    public void testGetSignature() {
        Assert.assertEquals("EjRWeJA=", authorizationHeader.getSignature());
    }

    @Test(expected = Exception.class)
    public void testInvalidAuthorizationHeader() throws Exception {
        new AuthorizationHeader("Invalid Header");
    }

    @Test(expected = Exception.class)
    public void testMalformedAuthorizationHeader() throws Exception {
        new AuthorizationHeader("Malformed, Header");
    }

}
