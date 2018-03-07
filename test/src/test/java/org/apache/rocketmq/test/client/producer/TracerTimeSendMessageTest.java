package org.apache.rocketmq.test.client.producer;

import static com.google.common.truth.Truth.assertThat;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.log4j.Logger;
import org.apache.rocketmq.client.hook.TracerTimeSendMessageHook;
import org.apache.rocketmq.common.ClientTracerTimeUtil;
import org.apache.rocketmq.common.TracerTime;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.apache.rocketmq.test.base.BaseConf;
import org.apache.rocketmq.test.client.producer.order.OrderMsgIT;
import org.apache.rocketmq.test.client.rmq.RMQNormalProducer;
import org.apache.rocketmq.test.factory.MQMessageFactory;
import org.apache.rocketmq.tools.command.stats.QueryTracerTimeCommand;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TracerTimeSendMessageTest extends BaseConf {

    private static Logger logger = Logger.getLogger(OrderMsgIT.class);
    private RMQNormalProducer producer = null;
    private String topic = null;

    @Before
    public void setUp() {
        topic = initTopic();
        logger.info(String.format("use topic: %s;", topic));
        producer = getProducer(nsAddr, topic);
    }

    @After
    public void tearDown() {
        shutDown();
    }

    @Test
    public void testOrderMsg_with_tracer() throws Exception {

        int msgSize = 1;

        System.setProperty(ClientTracerTimeUtil.MESSAGE_TRACER_TIME_ENABLE, "true");
        List<Object> messageList = MQMessageFactory.getMsg(topic, msgSize, "tag");

        Message message = (Message) messageList.get(0);

        producer.getProducer().getDefaultMQProducerImpl().registerSendMessageHook(new TracerTimeSendMessageHook());

        producer.send(message);

        Map<String, String> properties = message.getProperties();
        String messageTraceTimeId = properties.get(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX);
        assertThat(properties.containsKey(MessageConst.MESSAGE_CREATE_TIME)).isTrue();
        assertThat(properties.containsKey(MessageConst.MESSAGE_SEND_TIME)).isTrue();
        assertThat(properties.containsKey(MessageConst.RECEIVE_SEND_ACK_TIME)).isTrue();
        assertThat(messageTraceTimeId).isNotEmpty();

        TracerTime tracerTime = queryTracerTime(topic, messageTraceTimeId);

        assertThat(String.valueOf(tracerTime.getMessageCreateTime())).isEqualTo(properties.get(MessageConst.MESSAGE_CREATE_TIME));

    }

    @Test
    public void test_QueryTracerTimeCommand() throws Exception {
        int msgSize = 1;

        Field tracerTimeFiled = ClientTracerTimeUtil.class.getDeclaredField("isEnableMessageTracerTime");

        tracerTimeFiled.setAccessible(true);
        tracerTimeFiled.setBoolean(null, true);

        List<Object> messageList = MQMessageFactory.getMsg(topic, msgSize, "tag");

        Message message = (Message) messageList.get(0);

        producer.send(message);

        Map<String, String> properties = message.getProperties();
        String messageTraceTimeId = properties.get(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX);

        List<String> args = new ArrayList<>();
        args.add("-b");
        args.add(BaseConf.getBrokerController1().getBrokerAddr());
        args.add("-id");
        args.add(messageTraceTimeId);

        try {
            Options options = ServerUtil.buildCommandlineOptions(new Options());

            QueryTracerTimeCommand queryTracerTimeCommand = new QueryTracerTimeCommand();

            CommandLine commandLine =
                ServerUtil.parseCmdLine(queryTracerTimeCommand.commandName(), args.toArray(new String[0]), queryTracerTimeCommand.buildCommandlineOptions(options), new PosixParser());

            queryTracerTimeCommand.execute(commandLine, options, null);
        } catch (Exception e) {
            assertThat(Boolean.FALSE).isTrue();
        }
    }
}
