package com.github.yfocusy.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {
    public static void main(String[] args) {
        System.out.println("Hello world");
        // 1. create Producer properties
        Properties properties = new Properties();
        String bootstrapServers = "127.0.0.1:9092";
//        properties.setProperty("bootstrap.server", bootstrapServers);
//        properties.setProperty("key.serializer", StringSerializer.class.getName());
//        properties.setProperty("value.serializer",StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        System.out.println(StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        // 2. Create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        // 3. Create a producer record
        ProducerRecord<String, String> record = new ProducerRecord<String, String>("yuli-1st-topic","m101");
        // 4. send data - asynchronous
        producer.send(record);
        // 5.
        producer.flush();
        producer.close();

    }
}
