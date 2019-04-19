/*
Main entry for building native image
*/
package org.apache.rocketmq.client;

import java.util.Collections;
import java.util.List;

import org.apache.rocketmq.client.Producer.CRocketMQDirectives;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import org.graalvm.nativeimage.IsolateThread;
import org.graalvm.nativeimage.c.CContext;
import org.graalvm.nativeimage.c.function.CEntryPoint;
import org.graalvm.nativeimage.c.struct.CField;
import org.graalvm.nativeimage.c.struct.CStruct;
import org.graalvm.nativeimage.c.type.CCharPointer;
import org.graalvm.nativeimage.c.type.CTypeConversion;
import org.graalvm.word.PointerBase;

import com.alibaba.fastjson.parser.ParserConfig;
import com.oracle.svm.core.c.ProjectHeaderFile;

/**
 * @CContext annotation tells this class providing the context to interact with
 * C context. This is required to build as a SO library file, but not necessary to
 * build an executable file.
 * 
 * @author cengfeng.lzy
 *
 */
@CContext(CRocketMQDirectives.class)
public class Producer {
	static class CRocketMQDirectives implements CContext.Directives {

		@Override
		public List<String> getHeaderFiles() {
			/*
			 * The header file with the C declarations that are imported. Here we give the
			 * name of the header file. SVM searches the header file according to the file
			 * name specified here and the relative path specified in H:CLibraryPath in
			 * option.
			 */
			return Collections.singletonList(ProjectHeaderFile.resolve("client", "rocketMQ.h"));
		}
	}

	/**
	 * This interface gives a Java version description of Message_Send_Struct data
	 * structure defined in the C header file.
	 * 
	 * This declaration MUST be enclosed inside the @CContext class.
	 *  
	 * @author cengfeng.lzy
	 *
	 */
	@CStruct("Message_Send_Struct")
	interface CMessageSendStruct extends PointerBase {
		@CField("producer_name")
		CCharPointer getProducerName();

		@CField("producer_name")
		void setProducerName(CCharPointer value);

		@CField("topic")
		CCharPointer getTopic();

		@CField("topic")
		void setTopic(CCharPointer value);

		@CField("tags")
		CCharPointer getTags();

		@CField("tags")
		void setTags(CCharPointer value);

		@CField("keys")
		CCharPointer getKeys();

		@CField("keys")
		void setKeys(CCharPointer value);

		@CField("body")
		CCharPointer getBody();

		@CField("body")
		void setBody(CCharPointer value);
	}

	/**
	 * This main method is used to generate an executable file by SVM.
	 * @param args
	 * @throws MQClientException
	 * @throws InterruptedException
	 */
	public static void main(String[] args) throws MQClientException, InterruptedException {
		ParserConfig.global.setAsmEnable(false);
		DefaultMQProducer producer = new DefaultMQProducer("ProducerGroupName");
		producer.start();

		for (int i = 0; i < 128; i++)
			try {
				{
					Message msg = new Message("TopicTest", "TagA", "OrderID188",
							"Hello world".getBytes(RemotingHelper.DEFAULT_CHARSET));
					SendResult sendResult = producer.send(msg);
					System.out.printf("%s%n", sendResult);
				}

			} catch (Exception e) {
				e.printStackTrace();
			}

		producer.shutdown();
	}

	/**
	 * This example shows how to expose an API with complex data structure
	 * parameter. This API wraps SendResult
	 * org.apache.rocketmq.client.producer.DefaultMQProducer.send(Message msg)
	 * 
	 * @param thread        isolated thread is required by SVM
	 * @param cmessageSends correspond to the Message_Send_Struct defined in the C
	 *                      header file.
	 * @return CCharPointer corresponds to char * in C
	 */
	@CEntryPoint(name = "send_message")
	public static CCharPointer send(IsolateThread thread, CMessageSendStruct cmessageSends) {
		// Disable dynamic class generation and class loading in Fastjson
		ParserConfig.global.setAsmEnable(false);
		DefaultMQProducer producer = new DefaultMQProducer(
				// Here shows how to get a char * to String
				CTypeConversion.toJavaString(cmessageSends.getProducerName()));
		try {
			producer.start();
		} catch (MQClientException e1) {
			e1.printStackTrace();
			// Here shows how to convert null to char *.
			// As the returned value must be of WordBase type, but null is of Object type.
			// So we cannot return a null directly, but have to convert it to a
			// CCharPointer.
			return CTypeConversion.toCString(null).get();
		}

		String topic = CTypeConversion.toJavaString(cmessageSends.getTopic()); // TopicTest
		String tags = CTypeConversion.toJavaString(cmessageSends.getTags()); // TagA
		String key = CTypeConversion.toJavaString(cmessageSends.getKeys()); // OrderID188
		String body = CTypeConversion.toJavaString(cmessageSends.getBody()); // Hello world
		try {
			// Construct a Message instance from the data extracted from C structure.
			Message msg = new Message(topic, tags, key, body.getBytes(RemotingHelper.DEFAULT_CHARSET));
			SendResult sendResult = producer.send(msg);
			// Return string contents in SendResult instance.
			return CTypeConversion.toCString(sendResult.toString()).get();
		} catch (Exception e) {
			e.printStackTrace();
			return CTypeConversion.toCString(null).get();
		} finally {
			producer.shutdown();
		}
	}
}
