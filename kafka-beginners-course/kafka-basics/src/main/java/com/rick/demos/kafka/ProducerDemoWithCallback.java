package com.rick.demos.kafka;


import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * @Author: Rick
 * @Date: 2023/10/5 20:28
 */
public class ProducerDemoWithCallback {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithCallback.class.getSimpleName());

    public static void main(String[] args) {
        // log.info("Hello World!");
        log.info("I am a Kafka producer");

        // (1) create Producer Properties
        Properties properties = new Properties();

        // local: connect to localhost
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        // remote: connect to Conduktor Playground
        // properties.setProperty("bootstrap.servers", "cluster.playground.cdkt.io:9092");
        // properties.setProperty("security.protocol", "SAL_SSL");
        // properties.setProperty("sasl.jaas.config", "xxxxxxxxxxxx");
        // properties.setProperty("sasl.mechanism", "PLAIN");


        // set producer properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        // you can never go for small batch size in really production environment
        // default batch.size=16KB=16384Byte=131072Bit
        properties.setProperty("batch.size", "400");

        // this can make sure to observe different partitioner, but !!! HIGHLY NOT RECOMMENDED IN PRODUCTION !!!
        // properties.setProperty("partitioner.class", RoundRobinPartitioner.class.getName());

        // (2) create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for (int j = 0; j < 10; j++) {

            // create a Producer record - A key/value pair to be sent to Kafka.
            for (int i = 0; i < 30; i++) {
                ProducerRecord<String, String> producerRecord =
                        new ProducerRecord<>("demo_java", "hello world" + i);
                // (3) send data
                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception e) {
                        // executes every time a record successfully sent or an exception is thrown
                        if (e == null) {
                            log.info("Received new metadata \n" +
                                    "Topic: " + metadata.topic() + "\n" +
                                    "Partition: " + metadata.partition() + "\n" +
                                    "Offset: " + metadata.offset() + "\n" +
                                    "Timestamp: " + metadata.timestamp() + "\n");
                        } else {
                            log.error("Error while producing", e);
                        }
                    }
                });
            }

            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        // (4) flush and close the Producer
        // tell the producer to send all data and block until done - synchronous
        producer.flush();

        // flush and close the producer
        producer.close();
    }

}
