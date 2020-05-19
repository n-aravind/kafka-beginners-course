package com.github.anareddy.kafka.tutorial1.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {

    public static void main(String[] args) {

        String bootstrapServers = "127.0.0.1:9092";
        // Create Producer properties
        // https://kafka.apache.org/documentation/#producerconfigs

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        // Create Producer

        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);

        // Create a Producer Record

        ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic","Hello World");

        // Produce
        producer.send(record);

        //flush data to send it
        producer.flush();

        //close producer
        producer.close();
    }
}
