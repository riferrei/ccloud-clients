package io.confluent.devx.demo.clients;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import io.confluent.devx.demo.model.Order;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;

public class NativeConsumer {

  public void run(Properties consumerConfig) {

    consumer = new KafkaConsumer<String, Order>(consumerConfig);
    consumer.subscribe(Arrays.asList("orders"));
    ConsumerRecords<String, Order> records = null;
    
    while (true) {

      records = consumer.poll(Duration.ofMillis(500));

      for (ConsumerRecord<String, Order> record : records) {
        System.out.println(record.value());
      }

    }

  }

  private static KafkaConsumer<String, Order> consumer;

  static {

    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      consumer.close();
    }));

  }

  public static void main(String args[]) throws Exception {

    Properties consumerConfig = new Properties();

    consumerConfig.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
    consumerConfig.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
    consumerConfig.setProperty(ConsumerConfig.GROUP_ID_CONFIG, NativeConsumer.class.getSimpleName());
    consumerConfig.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
    consumerConfig.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    consumerConfig.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
    consumerConfig.load(NativeConsumer.class.getResourceAsStream("/ccloud.properties"));

    new NativeConsumer().run(consumerConfig);

  }

}