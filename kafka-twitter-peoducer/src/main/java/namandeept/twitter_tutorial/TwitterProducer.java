package namandeept.twitter_tutorial;


import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/*
 The twitter producer will run through these steps
   1. Establishing the connection and creating the twitter client
   2. Reading and processing data into our kafka topic assuming 6 partitions
      : For this in the CLI create a topic named twitter_tweets
      Run the command : kafka-topics --zookeeper localhost:2181 --create --topic twitter_tweets --partitions 6 --replication-factor 1
      After running both kafka and zookeeper server
 */


public class TwitterProducer {
    /*
        Secret key  for twitter developer application
     */
    private final String consumerKey = "YGja2mMBWZpuqwFKVLfW4HW7Y";
    private final String consumerSecret = "8dBCYo17chwHLCH9JtkdCmuauTzFbWOZYcNKZ0ksj0kWNOirk6";
    private final String token = "2703730705-UWLZs8UdNWB7bHsK44yIidjCVv702G1hvFDz4mb";
    private final String tokenSecret = "Vrkj2624YjazHSvuaGvYyzJzRRMXIXYV4GIlNLm3IIWTf";
    private final Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());

    public TwitterProducer(){} // A default constructor

    public static void main(String[] args) {
        TwitterProducer producer = new TwitterProducer();
        producer.run();
    }

    public void run(){
        //creating the client
        logger.info("Setup");

        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);
        Client client = createClient(msgQueue);
        KafkaProducer<String, String>kafkaProducer
                = createKafkaProducer();

        Runtime.getRuntime().addShutdownHook(
                new Thread(() -> {
                    logger.info("Stopping application.");
                    client.stop();
                    kafkaProducer.close();
        })
        );
        client.connect();
        while (!client.isDone()) {
            String msg = null;
            try{
                msg = msgQueue.poll(5, TimeUnit.MILLISECONDS);
            } catch(InterruptedException e){
                e.printStackTrace();
                client.stop();
            }
            if(msg!=null){
                logger.info(msg);
                ProducerRecord<String,String>producerRecord = new
                        ProducerRecord<>("twitter_tweets", msg);
                kafkaProducer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if(e!=null){
                            logger.info("Exception Raised",e);
                        }

                    }
                });
            }
        }
        logger.info("End Of Message");
    }

    public Client createClient(BlockingQueue<String>msgQueue){
        Hosts hosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
        // Optional: set up some followings and track terms
        List<String> terms = Lists.newArrayList("kafka");
        endpoint.trackTerms(terms);

        // These secrets should be read from a config file
        Authentication authentication = new OAuth1(consumerKey, consumerSecret, token, tokenSecret);

        ClientBuilder builder = new ClientBuilder()
                .name("client-01")     // optional: mainly for the logs
                .hosts(hosts)
                .authentication(authentication)
                .endpoint(endpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        Client client_ = builder.build();
        // Attempts to establish a connection.
        return client_;
    }

    public KafkaProducer<String,String> createKafkaProducer(){
        String bootstrap_server = "127.0.0.1:9092";
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrap_server);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        //creating producer
        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);
        return producer;
    }
}
