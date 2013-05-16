/**
 * $Id: ConsoleController.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.console;

import org.eclipse.jetty.server.Server;

import com.alibaba.rocketmq.console.http.ConsoleJettyHander;


/**
 * @author vintage.wang@gmail.com shijia.wxr@taobao.com
 */
public class ConsoleController {
    private final ConsoleConfig consoleConfig;
    private final Server jettyServer;
    private final ConsoleJettyHander consoleJettyHander;


    public ConsoleController(final ConsoleConfig consoleConfig) {
        this.consoleConfig = consoleConfig;
        this.consoleJettyHander = new ConsoleJettyHander();
        this.jettyServer = this.createJettyServer();
    }


    private Server createJettyServer() {
        Server server = new Server(this.consoleConfig.getListenPort());
        server.setHandler(this.consoleJettyHander);

        return server;
    }


    public void start() throws Exception {
        this.jettyServer.start();
    }


    public void shutdown() {
        try {
            this.jettyServer.stop();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }


    public ConsoleJettyHander getConsoleJettyHander() {
        return consoleJettyHander;
    }
}
