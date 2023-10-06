package com.rick.demos.kafka;


import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

/**
 * @Author: Rick
 * @Date: 2023/10/5 20:28
 */
public class ConsumerDemoShutdownConsumerGroup {

    private static final Logger log = LoggerFactory.getLogger(ConsumerDemoShutdownConsumerGroup.class.getSimpleName());

    public static void main(String[] args) {
        log.info("I am a Kafka Consumer!");

        String groupId = "my-java-application";
        String topic = "demo_java";


        // (1) create Producer Properties
        Properties properties = new Properties();

        // local: connect to localhost
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        // remote: connect to Conduktor Playground
        // properties.setProperty("bootstrap.servers", "cluster.playground.cdkt.io:9092");
        // properties.setProperty("security.protocol", "SAL_SSL");
        // properties.setProperty("sasl.jaas.config", "xxxxxxxxxxxx");
        // properties.setProperty("sasl.mechanism", "PLAIN");

        // create consumer configs
        // producer -> serializer
        // consumer -> deserializer
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());

        properties.setProperty("group.id", groupId);

        // none     - if we don't have any existing consumer group, then we failed
        // earliest - reading from beginning of the Topic --from-beginning
        // latest   - only reading the new msg sent from now
        properties.setProperty("auto.offset.reset", "earliest");

        // (2) create a consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // create a shutdown hook
        // get a reference to the main thread
        final Thread mainThread = Thread.currentThread();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                log.info("Detected a shutdown, let's exit by calling consumer.wakeup()...");
                consumer.wakeup(); // next time consumer.poll() will receive a wakeup exception

                // join the main thread to allow the execution of the code in main thread
                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });

        // (3) subscribe to a topic
        //      subscribe to a topic so we can consume msg
        try {


            consumer.subscribe(Arrays.asList(topic));

            // (4) poll for data
            while (true) {
                // if Kafka has no data return to us, we'll wait 1s to receive from Kafka
                ConsumerRecords<String, String> records =
                        consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record : records) {
                    log.info("Key: " + record.key() + ", Value " + record.value());
                    log.info("Partition: " + record.partition() + ", Offset: " + record.offset());
                }
            }
        } catch (WakeupException e){
            log.info("Consumer is starting to shut down");
        } catch (Exception e){
            log.error("Unexpected exception in the consumer", e);
        } finally {
            consumer.close(); // close the consumer, this will also commit offsets
            log.info("The consumer is now gracefully shutdown");
        }
    }

}
