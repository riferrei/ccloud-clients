package io.confluent.cloud.demo.clients;

import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.TopicExistsException;

public class Utils {

    public static final String ORDERS = "orders";

    public static void createTopic(Properties properties) {

        NewTopic newTopic = new NewTopic(ORDERS, 4, (short) 3);
        try (AdminClient adminClient = AdminClient.create(properties)) {
            adminClient.createTopics(Collections.singletonList(newTopic));
        } catch (TopicExistsException tee) {}

    }

}