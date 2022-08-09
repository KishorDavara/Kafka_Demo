package org.kafka.demo;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallBack {
    private static final Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallBack.class.getSimpleName());
    public static void main(String[] args) {
        logger.info("::Kafka Producer::");
        //create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        //create the producer
        KafkaProducer<String ,String> producer = new KafkaProducer<>(properties);

        // create a record
        ProducerRecord<String,String> producerRecord = new ProducerRecord<>("demo_java","Hello World");

        //send the data - asynchronous operation
        producer.send(producerRecord, new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                //executes every time a record is successfully sent or an exception is thrown
                if(exception == null) {
                    // the record was successfully sent
                    logger.info("Received new metadata/n \n" +
                            "Topic: " + metadata.topic() + "\n" +
                            "Partition: " + metadata.partition() + "\n" +
                            "Offset: " + metadata.offset() + "\n" +
                            "Timestamp: " + metadata.timestamp());
                } else {
                    logger.error("Error while producing the record ", exception);
                }
            }
        });

        //flush data - synchronous operation and close the producer
        //producer.flush(); // flush the data
        producer.close(); // flush and close
    }
}
