package com.github.namandeept.sample;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class consumerdemo {
    static final Logger logger = LoggerFactory.getLogger(consumerdemo.class);
    static final String bootstrap_server = "127.0.0.1:9092";
    static final String group_id = "my_application";
    static final String topic = "my_topic";
    static final String early_switch = "earliest";
    public static void main(String[] args) {

        //setting consumer properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrap_server);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());

        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,group_id);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,early_switch);


        KafkaConsumer<String,String>consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Arrays.asList(topic));

        while(true) {
            ConsumerRecords<String, String> consumerRecords =
                    consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String, String> record : consumerRecords) {
                logger.info("Key: " + record.key() + ",Value: " + record.value());
                logger.info("Partitions :" + record.partition() + ",offsets :" + record.offset());
            }
        }
    }
}
