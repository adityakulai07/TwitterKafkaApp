package com.adityaprojects.mykafkaproject;

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


public class TwitterProducer {

    private String consumerKey = "3y5t1qs4Jk2R1XvZVei9ufURt";
    private String consumerSecret = "6t87rlookkbVvTvpjem0XHYUgXNUoHm4tyZiZ9QFb15tpa6T4n";
    private String token = "1219597874230484992-7PkJFBheCSJOU1PVHg596AIYwHLAhI";
    private String secret = "egLMFqHiUrdbldPNfsWUWLNA9hSH0aBO9GrbZuEP2rVE9";

    Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());

    List<String> terms = Lists.newArrayList("india");

    public TwitterProducer(){}

    public void execute() {

        // Set up a message queue (Blocking queue)
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);

        // Create a Twitter client
        final Client client = createTwitterClient(msgQueue);

        // Attempt to establish connection
        client.connect();

        // Create a Kafka producer
        final KafkaProducer<String, String> producer = createKafkaProducer();

        // Adding a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Stopping app..");
            logger.info("Shutting down client from twitter..");
            client.stop();
            logger.info("Closing producer..");
            producer.close();
        }));

        // Loop to send tweets to Kafka cluster
        while(!client.isDone()) {
            String msg = null;
            try {
                msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
            }
            if(msg!=null) {
                logger.info(msg);
                producer.send(new ProducerRecord<String, String>("twitter_tweets", null, msg), new Callback() {
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if(e!=null) {
                            logger.error("Something is wrong", e);
                        }
                    }
                });
            }
            logger.info("End of Producer app");
        }
    }

    public Client createTwitterClient(BlockingQueue<String> msgQueue) {

        // Declare the host you want to connect to, the endpoint and the authentication
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

        // Set up for terms

        hosebirdEndpoint.trackTerms(terms);

        // Secrets need to be read from a config file
        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);

        // Create client
        ClientBuilder builder = new ClientBuilder()
                        .name("Hosebird-Client-01")
                        .hosts(hosebirdHosts)
                        .authentication(hosebirdAuth)
                        .endpoint(hosebirdEndpoint)
                        .processor(new StringDelimitedProcessor(msgQueue));

        Client hosebirdClient = builder.build();

        return hosebirdClient;
    }

    public KafkaProducer<String, String> createKafkaProducer() {
        String bootstrapServers = "127.0.0.1:9092";

        // Create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Create Producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        return producer;

    }

    public static void main(String[] args) {
        new TwitterProducer().execute();
    }
}
