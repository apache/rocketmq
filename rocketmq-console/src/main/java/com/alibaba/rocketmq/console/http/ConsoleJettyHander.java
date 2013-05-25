/**
 * $Id: ConsoleJettyHander.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.console.http;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;


/**
 * @author shijia.wxr<vintage.wang@gmail.com>
 * 
 */
public class ConsoleJettyHander extends AbstractHandler {

    @Override
    public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response)
            throws IOException, ServletException {
        System.out.println("---------------------------------------------------");
        System.out.println(target);
        System.out.println(baseRequest);
    }
}
