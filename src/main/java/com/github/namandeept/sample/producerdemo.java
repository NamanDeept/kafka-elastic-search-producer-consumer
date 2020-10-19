package com.github.namandeept.sample;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;

import java.util.Properties;

public class producerdemo {
    static final String bootstrap_server = "127.0.0.1:9092"; // default server for kafka application
    static final Logger logger = LoggerFactory.getLogger(producerdemo.class);

    public static void main(String[] args){

        //producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrap_server);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());


        //creating producer
        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);

        int i = 10;
        while(i-- >0){
            ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>("my_topic", "Hello World"+i);
            producer.send(producerRecord, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        String info = "Recived new Metadata :\n"
                                + "Topic Name :" + recordMetadata.topic() + "\n"
                                + "Partition :" + recordMetadata.partition() + "\n"
                                + "Offset :" + recordMetadata.partition() + "\n"
                                + "Timestamp :" + recordMetadata.timestamp();
                        logger.info(info);
                    } else {
                        String message = e.getMessage();
                        logger.error("Error generated", message);
                    }
                }
            });
        }

        producer.flush();
        producer.close();
    }
}
