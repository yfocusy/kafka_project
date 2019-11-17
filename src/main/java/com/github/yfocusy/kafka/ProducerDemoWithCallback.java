package com.github.yfocusy.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {
    public static void main(String[] args) {
        final Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);

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
        producer.send(record, new Callback() {
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                // executes every time a record is successfully sent or an exception is thrown
                if(e ==null){
                    logger.info("\n"+
                            "Received new metadata. \n"+
                            "Topic: "+recordMetadata.topic() +"\n"+
                            "Partition: "+recordMetadata.partition() +"\n"+
                            "Offset: " + recordMetadata.offset() +"\n"+
                            "Timestamp: " + recordMetadata.timestamp()
                    );

                }else{
                    logger.error("Error while producing", e);
                }
            }
        });
        // 5.
        producer.flush();
        producer.close();

    }
}
