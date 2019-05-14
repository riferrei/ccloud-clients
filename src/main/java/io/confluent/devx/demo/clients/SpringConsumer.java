package io.confluent.devx.demo.clients;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.stereotype.Service;

import io.confluent.devx.demo.model.Order;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;

@Configuration @Service
@SpringBootApplication
public class SpringConsumer {

    @KafkaListener(topics = "orders")
    public void consume(ConsumerRecord<String, Order> record) {
        System.out.println(record.value());
    }

    @Bean
    public ConsumerFactory<String, Order> consumerFactory() throws Exception {

        Map<String, Object> consumerConfig = new HashMap<String, Object>();

        consumerConfig.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        consumerConfig.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, SpringConsumer.class.getSimpleName());
        consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());

        InputStream is = SpringConsumer.class.getResourceAsStream("/ccloud.properties");
        Properties props = new Properties(); props.load(is);

        for (Object key : props.keySet()) {
            consumerConfig.put((String) key, props.get(key));
        }

        return new DefaultKafkaConsumerFactory<String, Order>(consumerConfig);

    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Order> kafkaListenerContainerFactory()
        throws Exception {
    
        ConcurrentKafkaListenerContainerFactory<String, Order> factory
            = new ConcurrentKafkaListenerContainerFactory<String, Order>();
        factory.setConsumerFactory(consumerFactory());

        return factory;

    }

    public static void main(String args[]) {
        SpringApplication.run(SpringConsumer.class, args);
    }    
    
}