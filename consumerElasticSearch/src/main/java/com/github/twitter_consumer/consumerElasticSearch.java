package com.github.twitter_consumer;


import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class consumerElasticSearch {

    public consumerElasticSearch(){} //default constructor

    /*
    key-URL: https://l33o29can1:lv89q60d1l@kafka-twitter-3954019433.us-east-1.bonsaisearch.net:443

    ID : 2gxZVnUBv93224yC_CD1 (uniquely generated puts on the data to the elastic search)
    custom-tweet-key : GET:twitter/tweets/ID
    */


    /* Global Variables for CreateClient() method */
    private final String username = "l33o29can1";
    private final String password = "lv89q60d1l";
    private final String host = "kafka-twitter-3954019433.us-east-1.bonsaisearch.net";
    private final int port_number = 443;
    private final String schema = "https";

    /*Global variables for createConsumer() method */
    private final String bootstrap_server = "localhost:9092";
    private final Logger logger = LoggerFactory.getLogger(consumerElasticSearch.class.getName());
    private final String group_id = "kafka_application_elasticSearch";
    private final String topic = "twitter_tweets";
    private final String early_switch = "earliest";

    /* JSON Parser */
    private final JsonParser parser = new JsonParser();


    public RestHighLevelClient CreateClient(){
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials(username,password));

        RestClientBuilder restClientBuilder = RestClient.builder(
                new HttpHost(host,port_number, schema));


        restClientBuilder.setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
            public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder) {
                return httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
            }
        });
        RestHighLevelClient client = new RestHighLevelClient(restClientBuilder);
        return client;
    }

    public KafkaConsumer<String,String> generateConsumer(){
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrap_server);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());

        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,group_id);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,early_switch);
        KafkaConsumer<String,String>consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Arrays.asList(topic));
        return consumer;
    }

    public String extractJSON(String JSON){
        String hash = "id_str";
        JsonElement output = parser.parse(JSON)
                        .getAsJsonObject()
                        .get("id_str");

        return output.getAsString();
    }

    public static void main(String[] args) throws IOException {
        Logger logger = LoggerFactory.getLogger(consumerElasticSearch.class.getName());
        consumerElasticSearch consumer = new consumerElasticSearch();
        RestHighLevelClient client = consumer.CreateClient();

        KafkaConsumer<String,String>kafkaconsumer = consumer.generateConsumer();
        while(true) {
            ConsumerRecords<String, String> consumerRecords =
                    kafkaconsumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String, String> record : consumerRecords) {
                String id = consumer.extractJSON(record.value());
                IndexRequest request = new IndexRequest(
                        "twitter","tweets",id //idempotence
                ).source(record.value(), XContentType.JSON);
                IndexResponse response = client.index(request, RequestOptions.DEFAULT);
                logger.info("hello"+ response.getId());
            }
        }

    }
}


