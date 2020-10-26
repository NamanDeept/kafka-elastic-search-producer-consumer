# kafka-elastic-search-producer-consumer
A list of tweets available throughout the generic topic search , the data being pulled into Kafka producer and searched efficiently by Kafka consumer using elastic Search.

Several codes are refactored and produced in separate files of the system.
## Prerequisites / Requirements : 
Setting up the kafka and the zookeeper server.
IntelliJ Idea / Eclipse or any other proper java IDE. 
A bonsai elasticsearch account for efficiently searching the given record using elasticsearch. 


In a cmd after running up the server, Used 6 partitions for the topic named twitter_tweets for smooth pipeline.
Run the given command in the new terminal: 
 "kafka-topics --zookeeper localhost:2181 --create --topic twitter_tweets --partitions 6 --replication-factor 1"
The default zookeeper service is embedded to localhost:2181 make sure that the given PORT is unused ,otherwise go to zookeeper.properties file and change the binding port number ((2181) is provided by default in kafka_2.0.0).

[Watch this video](https://www.youtube.com/watch?v=Bgjcuv-qkcg) for setting up bonsai elasticsearch. 
A bunch of keys will be available for your given cluster , copy the URL into consumerElasticSearch for data retrieval. 


Example : 
 key-URL: https://l33o29can1:lv89q60d1l@kafka-twitter-3954019433.us-east-1.bonsaisearch.net:443
    ID : 2gxZVnUBv93224yC_CD1 (uniquely generated puts on the data to the elastic search)
    custom-tweet-key : GET:twitter/tweets/ID


A given key-URL is defined as : 
          schema ://username:password@host:port_number replace the given code with the given key-URL generated. 
