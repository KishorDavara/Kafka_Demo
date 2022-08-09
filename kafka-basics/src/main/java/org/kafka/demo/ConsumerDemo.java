package org.kafka.demo;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemo {
    private static final Logger logger = LoggerFactory.getLogger(ConsumerDemo.class.getSimpleName());
    private static final String BOOTSTRAP_SERVER = "127.0.0.1:9092";
    private static final String GROUP_ID = "MY_FIRST_APPLICATION";
    private static final String OFFSET_RESET_CONFIG = "earliest";
    private static final String TOPIC = "demo_java";

    public static void main(String[] args) {
        logger.info("::Kafka Consumer::");

        //create consumer properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OFFSET_RESET_CONFIG);

        //create consumer
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);

        //subscribe consumer to our topic(s)
        kafkaConsumer.subscribe(Collections.singleton(TOPIC));

        //poll for new data
        while (true) {
            logger.info("Polling data...");
            ConsumerRecords<String, String> records =
                    kafkaConsumer.poll(Duration.ofMillis(1000)); // will return record immediate or wait till 1000 milliseconds
            for (ConsumerRecord<String, String> record : records) {
                logger.info("Key: " + record.key() + ", Value: " + record.value());
                logger.info("Partition: " + record.partition() + ", Offset: " + record.offset());
            }
        }
    }
}
