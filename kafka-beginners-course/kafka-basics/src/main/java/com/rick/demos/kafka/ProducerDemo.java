package com.rick.demos.kafka;



import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * @Author: Rick
 * @Date: 2023/10/5 20:28
 */
public class ProducerDemo {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());
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


        // (2) create the Producer
        // key is string and value also string
        // properties will tell kafka how to connect and how to serialize the key
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // create a Producer record - A key/value pair to be sent to Kafka.
        // need to create topic first
        // $kafka-topics.sh --bootstrap-server localhost:9092 --topic demo_java --create --partitions 3

        // check whether topic created
        // $ kafka-topics.sh --bootstrap-server localhost:9092 --list

        // verify it's successful
        // $ kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic demo_java --from-beginning
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo_java", "hello world");

        // (3) send data
        producer.send(producerRecord);

        // (4) flush and close the Producer
        // tell the producer to send all data and block until done - synchronous
        producer.flush();

        // flush and close the producer
        producer.close();
    }

}
