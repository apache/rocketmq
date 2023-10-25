package org.apache.rocketmq.store;

import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
public class MessageExtBrokerInnerTest {
    @Test
    public void testDeleteProperty() {
        MessageExtBrokerInner messageExtBrokerInner = new MessageExtBrokerInner();
        String propertiesString = "";
        messageExtBrokerInner.setPropertiesString(propertiesString);
        messageExtBrokerInner.deleteProperty("KeyA");
        assertThat(messageExtBrokerInner.getPropertiesString()).isEqualTo("");

        propertiesString = "KeyA\u0001ValueA";
        messageExtBrokerInner.setPropertiesString(propertiesString);
        messageExtBrokerInner.deleteProperty("KeyA");
        assertThat(messageExtBrokerInner.getPropertiesString()).isEqualTo("");

        propertiesString = "KeyA\u0001ValueA\u0002";
        messageExtBrokerInner.setPropertiesString(propertiesString);
        messageExtBrokerInner.deleteProperty("KeyA");
        assertThat(messageExtBrokerInner.getPropertiesString()).isEqualTo("");

        propertiesString = "KeyA\u0001ValueA\u0002KeyA\u0001ValueA";
        messageExtBrokerInner.setPropertiesString(propertiesString);
        messageExtBrokerInner.deleteProperty("KeyA");
        assertThat(messageExtBrokerInner.getPropertiesString()).isEqualTo("");

        propertiesString = "KeyA\u0001ValueA\u0002KeyA\u0001ValueA\u0002";
        messageExtBrokerInner.setPropertiesString(propertiesString);
        messageExtBrokerInner.deleteProperty("KeyA");
        assertThat(messageExtBrokerInner.getPropertiesString()).isEqualTo("");

        propertiesString = "KeyB\u0001ValueB\u0002KeyA\u0001ValueA";
        messageExtBrokerInner.setPropertiesString(propertiesString);
        messageExtBrokerInner.deleteProperty("KeyA");
        assertThat(messageExtBrokerInner.getPropertiesString()).isEqualTo("KeyB\u0001ValueB\u0002");

        propertiesString = "KeyB\u0001ValueB\u0002KeyA\u0001ValueA\u0002";
        messageExtBrokerInner.setPropertiesString(propertiesString);
        messageExtBrokerInner.deleteProperty("KeyA");
        assertThat(messageExtBrokerInner.getPropertiesString()).isEqualTo("KeyB\u0001ValueB\u0002");

        propertiesString = "KeyB\u0001ValueB\u0002KeyA\u0001ValueA\u0002KeyB\u0001ValueB\u0002";
        messageExtBrokerInner.setPropertiesString(propertiesString);
        messageExtBrokerInner.deleteProperty("KeyA");
        assertThat(messageExtBrokerInner.getPropertiesString()).isEqualTo("KeyB\u0001ValueB\u0002KeyB\u0001ValueB\u0002");

        propertiesString = "KeyA\u0001ValueA\u0002KeyB\u0001ValueB\u0002";
        messageExtBrokerInner.setPropertiesString(propertiesString);
        messageExtBrokerInner.deleteProperty("KeyA");
        assertThat(messageExtBrokerInner.getPropertiesString()).isEqualTo("KeyB\u0001ValueB\u0002");

        propertiesString = "KeyA\u0001ValueA\u0002KeyB\u0001ValueB";
        messageExtBrokerInner.setPropertiesString(propertiesString);
        messageExtBrokerInner.deleteProperty("KeyA");
        assertThat(messageExtBrokerInner.getPropertiesString()).isEqualTo("KeyB\u0001ValueB");

        propertiesString = "KeyA\u0001ValueA\u0002KeyB\u0001ValueBKeyA\u0001ValueA\u0002";
        messageExtBrokerInner.setPropertiesString(propertiesString);
        messageExtBrokerInner.deleteProperty("KeyA");
        assertThat(messageExtBrokerInner.getPropertiesString()).isEqualTo("KeyB\u0001ValueBKeyA\u0001ValueA\u0002");

        propertiesString = "KeyA\u0001ValueA\u0002KeyB\u0001ValueBKeyA\u0001";
        messageExtBrokerInner.setPropertiesString(propertiesString);
        messageExtBrokerInner.deleteProperty("KeyA");
        assertThat(messageExtBrokerInner.getPropertiesString()).isEqualTo("KeyB\u0001ValueBKeyA\u0001");

        propertiesString = "KeyA\u0001ValueA\u0002KeyB\u0001ValueBKeyA";
        messageExtBrokerInner.setPropertiesString(propertiesString);
        messageExtBrokerInner.deleteProperty("KeyA");
        assertThat(messageExtBrokerInner.getPropertiesString()).isEqualTo("KeyB\u0001ValueBKeyA");

        propertiesString = "__CRC32#\u0001";
        messageExtBrokerInner.setPropertiesString(propertiesString);
        messageExtBrokerInner.deleteProperty("__CRC32#");
        assertThat(messageExtBrokerInner.getPropertiesString()).isEmpty();

        propertiesString = "__CRC32#";
        messageExtBrokerInner.setPropertiesString(propertiesString);
        messageExtBrokerInner.deleteProperty("__CRC32#");
        assertThat(messageExtBrokerInner.getPropertiesString()).isEqualTo(propertiesString);
    }


}
