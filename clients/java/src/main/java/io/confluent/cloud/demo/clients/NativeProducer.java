package io.confluent.cloud.demo.clients;

import java.util.Date;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import io.confluent.cloud.demo.clients.model.Order;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import static io.confluent.cloud.demo.clients.Utils.*;

public class NativeProducer {

  private void run(Properties properties) {

    createTopic(properties);
    producer = new KafkaProducer<String, Order>(properties);
    ProducerRecord<String, Order> record = null;

    for (;;) {

      String generatedKey = UUID.randomUUID().toString();
      record = new ProducerRecord<String, Order>(ORDERS,
        generatedKey, createOrder(generatedKey));

      producer.send(record, new Callback() {

        @Override
        public void onCompletion(RecordMetadata metadata, Exception exception) {
          System.out.println("Order '" + generatedKey + "' created successfully!");
        }
        
      });

      try {
        Thread.sleep(100);
      } catch (InterruptedException ie) {}

    }

  }

  private Order createOrder(String generatedKey) {

    Order order = new Order();
    order.setId(generatedKey);
    order.setDate(new Date().getTime());
    order.setAmount(Double.valueOf(random.nextInt(1000)));

    return order;

  }

  private static final Random random = new Random();
  private static KafkaProducer<String, Order> producer;

  static {

    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      producer.close();
    }));

  }

  public static void main(String args[]) throws Exception {

    Properties properties = new Properties();

    properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
    properties.load(NativeProducer.class.getResourceAsStream("/ccloud.properties"));

    new NativeProducer().run(properties);

  }

}