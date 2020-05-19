package com.github.anareddy.kafka.tutorial1.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {

    public static void main(String[] args) {

        final Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);

        String bootstrapServers = "127.0.0.1:9092";
        // Create Producer properties
        // https://kafka.apache.org/documentation/#producerconfigs

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        // Create Producer
        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);


        // Produce
        for (int i = 0; i < 10 ; i++) {
            // Create a Producer Record
            ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic","Hello World " + i);

            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    //executes every time a record is successfully sent or an exception is thrown
                    if (e == null) {
                        // When record is successfully sent
                        logger.info("Received new metadata. \n" +
                                "Topic : " + recordMetadata.topic() + "\n" +
                                "Partition : " + recordMetadata.partition() + "\n" +
                                "Offset : " + recordMetadata.offset() + "\n" +
                                "Timestamp : " + recordMetadata.timestamp());
                    } else {
                        logger.error("Error while producing");
                    }
                }
            });
        }

        //flush data to send it
        producer.flush();

        //close producer
        producer.close();
    }
}
