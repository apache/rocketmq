package org.apache.rocketmq.namesrv.telnet;



import org.junit.Test;




public class TelnetServerTest {
	private static String addr = "192.168.185.172:9876";
	static {
		TelnetCommandUtil.setNamseSrvAddr(addr);
	}
	
	@Test
	public void testTelnetServer(){
		System.out.println("hello");
		TelnetServer server = new TelnetServer(8090);
		try {
			server.start();
			System.out.println("start end");
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//for(;;);
	}
	

}
