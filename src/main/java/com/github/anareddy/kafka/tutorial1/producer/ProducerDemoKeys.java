package com.github.anareddy.kafka.tutorial1.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        final Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);

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

            String topic = "first_topic";
            String value = "hello world " + i;
            String key = "id_" + i;

            // Create a Producer Record
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic,key,value);

            logger.info("Key" + key);

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
            }).get(); //block the .send() to make it synchronous - don't do this in production
        }

        //flush data to send it
        producer.flush();

        //close producer
        producer.close();
    }
}
